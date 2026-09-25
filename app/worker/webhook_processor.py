from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.clients.webhook import (
    WebhookClient,
    WebhookPermanentError,
    WebhookRetryableError,
)
from app.models.payment import PaymentStatus
from app.repositories.payment import PaymentRepository
from app.schemas.event import EventEnvelope, EventType
from app.schemas.payment import Currency
from app.schemas.webhook import (
    WebhookEventType,
    WebhookPayload,
)
from app.worker.consumer import (
    PermanentEventError,
    RetryableEventError,
)


@dataclass(frozen=True, slots=True)
class WebhookDelivery:
    url: str
    payload: WebhookPayload


class WebhookEventProcessor:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        webhook_client: WebhookClient,
    ) -> None:
        self._session_factory = session_factory
        self._webhook_client = webhook_client

    async def process(self, event: EventEnvelope) -> None:
        if event.event_type != EventType.WEBHOOK_DELIVERY_REQUESTED:
            raise PermanentEventError(f"Unsupported event type: {event.event_type.value}")

        delivery = await self._load_delivery(event)

        if delivery is None:
            return

        try:
            await self._webhook_client.deliver(
                url=delivery.url,
                payload=delivery.payload,
                event_id=event.event_id,
                attempt=event.attempt,
            )
        except WebhookRetryableError as exc:
            raise RetryableEventError("Webhook delivery failed with retryable error") from exc
        except WebhookPermanentError as exc:
            raise PermanentEventError("Webhook delivery failed permanently") from exc

        await self._mark_sent(payment_id=event.payment_id, event_id=event.event_id)

    async def _load_delivery(self, event: EventEnvelope) -> WebhookDelivery | None:
        async with self._session_factory() as session:
            repository = PaymentRepository(session)

            payment = await repository.get_by_id(event.payment_id)

            if payment is None:
                raise PermanentEventError(f"Payment {event.payment_id} was not found")

            if payment.webhook_sent_at is not None:
                return None

            if payment.status not in (
                PaymentStatus.SUCCEEDED,
                PaymentStatus.FAILED,
            ):
                raise PermanentEventError(
                    "Webhook cannot be delivered for a non-terminal payment",
                )

            if payment.webhook_event_id is None:
                raise PermanentEventError("Payment does not have webhook_event_id")

            if payment.webhook_event_id != event.event_id:
                raise PermanentEventError("Webhook event_id does not match payment")

            if payment.processed_at is None:
                raise PermanentEventError("Terminal payment does not have processed_at")

            if payment.status == PaymentStatus.SUCCEEDED:
                webhook_event_type = WebhookEventType.PAYMENT_SUCCEEDED
            else:
                webhook_event_type = WebhookEventType.PAYMENT_FAILED

            payload = WebhookPayload(
                event_id=payment.webhook_event_id,
                event_type=webhook_event_type,
                payment_id=payment.id,
                status=payment.status,
                amount=payment.amount,
                currency=Currency(payment.currency),
                processed_at=payment.processed_at,
                metadata=payment.payment_metadata,
            )

            return WebhookDelivery(url=payment.webhook_url, payload=payload)

    async def _mark_sent(self, *, payment_id: UUID, event_id: UUID) -> None:
        async with self._session_factory() as session:
            repository = PaymentRepository(session)

            try:
                payment = await repository.get_by_id_for_update(payment_id)

                if payment is None:
                    raise PermanentEventError(f"Payment {payment_id} was not found")

                if payment.webhook_sent_at is not None:
                    return

                if payment.webhook_event_id != event_id:
                    raise PermanentEventError("Webhook event_id does not match payment")

                if payment.status not in (
                    PaymentStatus.SUCCEEDED,
                    PaymentStatus.FAILED,
                ):
                    raise PermanentEventError("Webhook payment is no longer terminal")

                payment.webhook_sent_at = datetime.now(UTC)

                await session.commit()

            except Exception:
                await session.rollback()
                raise
