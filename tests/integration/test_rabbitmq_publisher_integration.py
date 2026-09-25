import json
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from app.core.config import get_settings
from app.integrations.rabbitmq.broker import create_rabbit_broker
from app.integrations.rabbitmq.declaration import (
    declare_rabbitmq_topology,
)
from app.integrations.rabbitmq.publisher import RabbitEventPublisher
from app.integrations.rabbitmq.topology import PAYMENTS_NEW_QUEUE
from app.schemas.event import EventEnvelope, EventType


@pytest.mark.integration
async def test_publisher_delivers_event_to_payments_new_queue() -> None:
    settings = get_settings()

    broker = create_rabbit_broker(settings)

    await broker.start()

    try:
        await declare_rabbitmq_topology(broker)

        queue = await broker.declare_queue(PAYMENTS_NEW_QUEUE)
        await queue.purge()

        event = EventEnvelope(
            event_id=uuid4(),
            event_type=EventType.PAYMENT_PROCESS_REQUESTED,
            payment_id=uuid4(),
            occurred_at=datetime.now(UTC),
        )

        publisher = RabbitEventPublisher(
            broker,
            timeout_seconds=settings.rabbit_publish_timeout_seconds,
        )

        await publisher.publish(event)

        message = await queue.get(
            timeout=2.0,
            fail=False,
        )

        assert message is not None

        try:
            payload = json.loads(message.body)

            assert payload["event_id"] == str(event.event_id)
            assert payload["event_type"] == event.event_type.value
            assert payload["schema_version"] == 1
            assert payload["payment_id"] == str(event.payment_id)
            assert payload["attempt"] == 1

            assert message.message_id == str(event.event_id)
            assert message.correlation_id == str(event.payment_id)
            assert message.type == event.event_type.value
        finally:
            await message.ack()
    finally:
        await broker.stop()
