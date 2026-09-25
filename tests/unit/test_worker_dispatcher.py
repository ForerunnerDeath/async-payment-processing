from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

from app.schemas.event import EventEnvelope, EventType
from app.worker.dispatcher import WorkerEventDispatcher
from app.worker.payment_processor import PaymentEventProcessor
from app.worker.webhook_processor import WebhookEventProcessor


def make_event(
    event_type: EventType,
) -> EventEnvelope:
    return EventEnvelope(
        event_id=uuid4(),
        event_type=event_type,
        payment_id=uuid4(),
        attempt=1,
        occurred_at=datetime(
            2026,
            9,
            25,
            12,
            0,
            tzinfo=UTC,
        ),
    )


async def test_dispatches_payment_processing_event() -> None:
    payment_processor = MagicMock(
        spec=PaymentEventProcessor,
    )
    payment_processor.process = AsyncMock()

    webhook_processor = MagicMock(
        spec=WebhookEventProcessor,
    )
    webhook_processor.process = AsyncMock()

    dispatcher = WorkerEventDispatcher(
        payment_processor,
        webhook_processor,
    )

    event = make_event(
        EventType.PAYMENT_PROCESS_REQUESTED,
    )

    await dispatcher.process(event)

    payment_processor.process.assert_awaited_once_with(
        event,
    )
    webhook_processor.process.assert_not_awaited()


async def test_dispatches_webhook_delivery_event() -> None:
    payment_processor = MagicMock(
        spec=PaymentEventProcessor,
    )
    payment_processor.process = AsyncMock()

    webhook_processor = MagicMock(
        spec=WebhookEventProcessor,
    )
    webhook_processor.process = AsyncMock()

    dispatcher = WorkerEventDispatcher(
        payment_processor,
        webhook_processor,
    )

    event = make_event(
        EventType.WEBHOOK_DELIVERY_REQUESTED,
    )

    await dispatcher.process(event)

    webhook_processor.process.assert_awaited_once_with(
        event,
    )
    payment_processor.process.assert_not_awaited()
