from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest
from faststream.rabbit import RabbitBroker

from app.integrations.rabbitmq.publisher import RabbitEventPublisher
from app.integrations.rabbitmq.topology import (
    PAYMENTS_EXCHANGE,
    PAYMENTS_NEW_QUEUE,
)
from app.schemas.event import EventEnvelope, EventType

EVENT_ID = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
PAYMENT_ID = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
OCCURRED_AT = datetime(2026, 9, 25, 9, 0, tzinfo=UTC)


def make_event() -> EventEnvelope:
    return EventEnvelope(
        event_id=EVENT_ID,
        event_type=EventType.PAYMENT_PROCESS_REQUESTED,
        payment_id=PAYMENT_ID,
        occurred_at=OCCURRED_AT,
    )


async def test_publish_sends_persistent_confirmed_message() -> None:
    broker = MagicMock(spec=RabbitBroker)
    broker.publish = AsyncMock()

    publisher = RabbitEventPublisher(
        broker,
        timeout_seconds=5.0,
    )

    event = make_event()

    await publisher.publish(event)

    broker.publish.assert_awaited_once_with(
        event.model_dump(mode="json"),
        exchange=PAYMENTS_EXCHANGE,
        routing_key=PAYMENTS_NEW_QUEUE.routing_key,
        mandatory=True,
        persist=True,
        timeout=5.0,
        message_id=str(EVENT_ID),
        correlation_id=str(PAYMENT_ID),
        message_type="payment.process_requested",
    )


async def test_publish_propagates_broker_error() -> None:
    broker = MagicMock(spec=RabbitBroker)
    broker.publish = AsyncMock(
        side_effect=TimeoutError("publish timed out"),
    )

    publisher = RabbitEventPublisher(
        broker,
        timeout_seconds=5.0,
    )

    event = make_event()

    with pytest.raises(
        TimeoutError,
        match="publish timed out",
    ):
        await publisher.publish(event)
