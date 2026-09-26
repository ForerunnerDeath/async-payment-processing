import asyncio
import json
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from aio_pika.abc import AbstractIncomingMessage, AbstractQueue
from sqlalchemy.exc import SQLAlchemyError

from app.core.config import get_settings
from app.integrations.rabbitmq.broker import create_rabbit_broker
from app.integrations.rabbitmq.declaration import declare_rabbitmq_topology
from app.integrations.rabbitmq.publisher import RabbitEventPublisher
from app.integrations.rabbitmq.topology import (
    PAYMENTS_DLQ,
    PAYMENTS_NEW_QUEUE,
    PAYMENTS_RETRY_1_QUEUE,
    PAYMENTS_RETRY_2_QUEUE,
)
from app.schemas.event import EventEnvelope, EventType
from app.worker.consumer import PaymentQueueConsumer
from app.worker.subscriber import register_payment_queue_subscriber


class RecoveringProcessor:
    def __init__(self) -> None:
        self.attempts: list[int] = []
        self.completed = asyncio.Event()

    async def process(self, event: EventEnvelope) -> None:
        self.attempts.append(event.attempt)

        if event.attempt == 1:
            raise SQLAlchemyError("database is temporarily unavailable")

        self.completed.set()


class FailingProcessor:
    def __init__(self) -> None:
        self.attempts: list[int] = []

    async def process(self, event: EventEnvelope) -> None:
        self.attempts.append(event.attempt)

        raise SQLAlchemyError("database remains unavailable")


async def wait_for_queue_message(
    queue: AbstractQueue,
    *,
    timeout_seconds: float,
) -> AbstractIncomingMessage | None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_seconds

    while True:
        message = await queue.get(
            timeout=1.0,
            fail=False,
        )

        if message is not None:
            return message

        remaining = deadline - loop.time()

        if remaining <= 0:
            return None

        await asyncio.sleep(
            min(0.05, remaining),
        )


@pytest.mark.integration
async def test_unexpected_failure_is_retried_without_consumer_restart() -> None:
    settings = get_settings()

    broker = create_rabbit_broker(settings)

    processor = RecoveringProcessor()

    publisher = RabbitEventPublisher(
        broker,
        timeout_seconds=settings.rabbit_publish_timeout_seconds,
    )

    consumer = PaymentQueueConsumer(
        processor,
        publisher,
    )

    register_payment_queue_subscriber(
        broker,
        consumer,
    )

    await broker.connect()

    try:
        await declare_rabbitmq_topology(broker)

        new_queue = await broker.declare_queue(PAYMENTS_NEW_QUEUE)
        retry_1_queue = await broker.declare_queue(PAYMENTS_RETRY_1_QUEUE)
        retry_2_queue = await broker.declare_queue(PAYMENTS_RETRY_2_QUEUE)
        dlq = await broker.declare_queue(PAYMENTS_DLQ)

        await new_queue.purge()
        await retry_1_queue.purge()
        await retry_2_queue.purge()
        await dlq.purge()

        await broker.start()

        event = EventEnvelope(
            event_id=uuid4(),
            event_type=EventType.PAYMENT_PROCESS_REQUESTED,
            payment_id=uuid4(),
            attempt=1,
            occurred_at=datetime.now(UTC),
        )

        await publisher.publish(event)

        await asyncio.wait_for(
            processor.completed.wait(),
            timeout=5.0,
        )

        assert processor.attempts == [1, 2]

    finally:
        await broker.stop()


@pytest.mark.integration
async def test_persistent_unexpected_failure_reaches_dlq_after_max_attempts() -> None:
    settings = get_settings()

    broker = create_rabbit_broker(settings)

    processor = FailingProcessor()

    publisher = RabbitEventPublisher(
        broker,
        timeout_seconds=settings.rabbit_publish_timeout_seconds,
    )

    consumer = PaymentQueueConsumer(
        processor,
        publisher,
    )

    register_payment_queue_subscriber(
        broker,
        consumer,
    )

    await broker.connect()

    try:
        await declare_rabbitmq_topology(broker)

        new_queue = await broker.declare_queue(PAYMENTS_NEW_QUEUE)
        retry_1_queue = await broker.declare_queue(PAYMENTS_RETRY_1_QUEUE)
        retry_2_queue = await broker.declare_queue(PAYMENTS_RETRY_2_QUEUE)
        dlq = await broker.declare_queue(PAYMENTS_DLQ)

        await new_queue.purge()
        await retry_1_queue.purge()
        await retry_2_queue.purge()
        await dlq.purge()

        await broker.start()

        event = EventEnvelope(
            event_id=uuid4(),
            event_type=EventType.PAYMENT_PROCESS_REQUESTED,
            payment_id=uuid4(),
            attempt=1,
            occurred_at=datetime.now(UTC),
        )

        await publisher.publish(event)

        dead_letter = await wait_for_queue_message(
            dlq,
            timeout_seconds=7.0,
        )

        assert dead_letter is not None

        try:
            payload = json.loads(dead_letter.body)

            assert processor.attempts == [1, 2, 3]

            assert payload["event_id"] == str(event.event_id)
            assert payload["payment_id"] == str(event.payment_id)
            assert payload["event_type"] == event.event_type.value
            assert payload["attempt"] == 3
        finally:
            await dead_letter.ack()

    finally:
        await broker.stop()
