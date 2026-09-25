from app.schemas.event import EventEnvelope, EventType
from app.worker.consumer import PermanentEventError
from app.worker.payment_processor import PaymentEventProcessor
from app.worker.webhook_processor import WebhookEventProcessor


class WorkerEventDispatcher:
    def __init__(
        self, payment_processor: PaymentEventProcessor, webhook_processor: WebhookEventProcessor
    ) -> None:
        self._payment_processor = payment_processor
        self._webhook_processor = webhook_processor

    async def process(self, event: EventEnvelope) -> None:
        match event.event_type:
            case EventType.PAYMENT_PROCESS_REQUESTED:
                await self._payment_processor.process(event)

            case EventType.WEBHOOK_DELIVERY_REQUESTED:
                await self._webhook_processor.process(event)

            case _:
                raise PermanentEventError(
                    f"Unsupported event type: {event.event_type}",
                )
