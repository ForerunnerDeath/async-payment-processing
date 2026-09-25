from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest
from faststream.rabbit import RabbitMessage

from app.integrations.rabbitmq.publisher import RabbitEventPublisher
from app.schemas.event import EventEnvelope, EventType
from app.worker.consumer import (
    EventProcessor,
    PaymentQueueConsumer,
    PermanentEventError,
    RetryableEventError,
)

EVENT_ID = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
PAYMENT_ID = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")


def make_event(
    *,
    attempt: int = 1,
) -> EventEnvelope:
    return EventEnvelope(
        event_id=EVENT_ID,
        event_type=EventType.PAYMENT_PROCESS_REQUESTED,
        payment_id=PAYMENT_ID,
        attempt=attempt,
        occurred_at=datetime(
            2026,
            9,
            25,
            10,
            0,
            tzinfo=UTC,
        ),
    )


def make_message() -> MagicMock:
    message = MagicMock(spec=RabbitMessage)
    message.ack = AsyncMock()
    message.reject = AsyncMock()
    message.nack = AsyncMock()

    return message


async def test_consumer_acks_successful_event() -> None:
    processor = MagicMock(spec=EventProcessor)
    processor.process = AsyncMock()

    publisher = MagicMock(spec=RabbitEventPublisher)
    publisher.publish_retry = AsyncMock()

    message = make_message()
    event = make_event()

    consumer = PaymentQueueConsumer(
        processor,
        publisher,
    )

    await consumer.handle(
        event.model_dump(mode="json"),
        message,
    )

    processor.process.assert_awaited_once_with(event)

    message.ack.assert_awaited_once_with()
    message.reject.assert_not_awaited()
    publisher.publish_retry.assert_not_awaited()


async def test_consumer_rejects_invalid_envelope() -> None:
    processor = MagicMock(spec=EventProcessor)
    processor.process = AsyncMock()

    publisher = MagicMock(spec=RabbitEventPublisher)

    message = make_message()

    consumer = PaymentQueueConsumer(
        processor,
        publisher,
    )

    await consumer.handle(
        {
            "event_type": "not-a-real-event",
        },
        message,
    )

    processor.process.assert_not_awaited()

    message.reject.assert_awaited_once_with()
    message.ack.assert_not_awaited()


async def test_consumer_rejects_permanent_failure() -> None:
    processor = MagicMock(spec=EventProcessor)
    processor.process = AsyncMock(
        side_effect=PermanentEventError(
            "invalid payment",
        ),
    )

    publisher = MagicMock(spec=RabbitEventPublisher)

    message = make_message()
    event = make_event()

    consumer = PaymentQueueConsumer(
        processor,
        publisher,
    )

    await consumer.handle(
        event.model_dump(mode="json"),
        message,
    )

    message.reject.assert_awaited_once_with()
    message.ack.assert_not_awaited()


async def test_consumer_schedules_first_retry_and_acks_original() -> None:
    processor = MagicMock(spec=EventProcessor)
    processor.process = AsyncMock(
        side_effect=RetryableEventError(
            "provider unavailable",
        ),
    )

    publisher = MagicMock(spec=RabbitEventPublisher)
    publisher.publish_retry = AsyncMock()

    message = make_message()
    event = make_event(attempt=1)

    consumer = PaymentQueueConsumer(
        processor,
        publisher,
    )

    await consumer.handle(
        event.model_dump(mode="json"),
        message,
    )

    publisher.publish_retry.assert_awaited_once()

    retry_event = publisher.publish_retry.await_args.args[0]

    assert isinstance(retry_event, EventEnvelope)
    assert retry_event.event_id == event.event_id
    assert retry_event.payment_id == event.payment_id
    assert retry_event.attempt == 2
    assert retry_event.occurred_at == event.occurred_at

    message.ack.assert_awaited_once_with()
    message.reject.assert_not_awaited()


async def test_consumer_rejects_after_third_failed_attempt() -> None:
    processor = MagicMock(spec=EventProcessor)
    processor.process = AsyncMock(
        side_effect=RetryableEventError(
            "provider unavailable",
        ),
    )

    publisher = MagicMock(spec=RabbitEventPublisher)
    publisher.publish_retry = AsyncMock()

    message = make_message()
    event = make_event(attempt=3)

    consumer = PaymentQueueConsumer(
        processor,
        publisher,
    )

    await consumer.handle(
        event.model_dump(mode="json"),
        message,
    )

    publisher.publish_retry.assert_not_awaited()

    message.reject.assert_awaited_once_with()
    message.ack.assert_not_awaited()


async def test_consumer_does_not_ack_when_retry_publish_fails() -> None:
    processor = MagicMock(spec=EventProcessor)
    processor.process = AsyncMock(
        side_effect=RetryableEventError(
            "provider unavailable",
        ),
    )

    publisher = MagicMock(spec=RabbitEventPublisher)
    publisher.publish_retry = AsyncMock(
        side_effect=RuntimeError(
            "RabbitMQ unavailable",
        ),
    )

    message = make_message()
    event = make_event()

    consumer = PaymentQueueConsumer(
        processor,
        publisher,
    )

    with pytest.raises(
        RuntimeError,
        match="RabbitMQ unavailable",
    ):
        await consumer.handle(
            event.model_dump(mode="json"),
            message,
        )

    message.ack.assert_not_awaited()
    message.reject.assert_not_awaited()


async def test_consumer_propagates_unexpected_error_without_acknowledging() -> None:
    processor = MagicMock(spec=EventProcessor)
    processor.process = AsyncMock(
        side_effect=RuntimeError(
            "unexpected failure",
        ),
    )

    publisher = MagicMock(spec=RabbitEventPublisher)

    message = make_message()
    event = make_event()

    consumer = PaymentQueueConsumer(
        processor,
        publisher,
    )

    with pytest.raises(
        RuntimeError,
        match="unexpected failure",
    ):
        await consumer.handle(
            event.model_dump(mode="json"),
            message,
        )

    message.ack.assert_not_awaited()
    message.reject.assert_not_awaited()
    message.nack.assert_not_awaited()
