from datetime import UTC, datetime
from uuid import UUID, uuid4

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.clients.payment_provider import (
    PaymentProviderClient,
    ProviderAmbiguousOutcomeError,
    ProviderPermanentError,
    ProviderUnavailableError,
)
from app.models.outbox_event import OutboxEvent
from app.models.payment import PaymentStatus
from app.repositories.outbox import OutboxRepository
from app.repositories.payment import PaymentRepository
from app.resilience.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerOpenError,
)
from app.schemas.event import (
    EventEnvelope,
    EventType,
)
from app.schemas.payment import Currency
from app.schemas.provider import (
    ProviderPaymentRequest,
    ProviderPaymentResponse,
    ProviderPaymentStatus,
)
from app.worker.consumer import (
    PermanentEventError,
    RetryableEventError,
)


class PaymentEventProcessor:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        provider_client: PaymentProviderClient,
        circuit_breaker: CircuitBreaker,
    ) -> None:
        self._session_factory = session_factory
        self._provider_client = provider_client
        self._circuit_breaker = circuit_breaker

    async def process(self, event: EventEnvelope) -> None:
        if event.event_type != EventType.PAYMENT_PROCESS_REQUESTED:
            raise PermanentEventError(
                f"Unsupported event type: {event.event_type.value}",
            )

        provider_request = await self._load_provider_request(event.payment_id)

        if provider_request is None:
            return

        try:
            provider_response = await self._circuit_breaker.call(
                self._provider_client.process_payment,
                provider_request,
            )
        except CircuitBreakerOpenError as exc:
            raise RetryableEventError("Payment provider circuit breaker is open") from exc
        except ProviderUnavailableError as exc:
            raise RetryableEventError("Payment provider is unavailable") from exc
        except ProviderAmbiguousOutcomeError:
            await self._mark_unknown(event.payment_id)

            return
        except ProviderPermanentError as exc:
            raise PermanentEventError(
                "Payment provider permanently rejected processing request",
            ) from exc

        await self._apply_provider_result(
            payment_id=event.payment_id,
            provider_response=provider_response,
        )

    async def _load_provider_request(self, payment_id: UUID) -> ProviderPaymentRequest | None:
        async with self._session_factory() as session:
            repository = PaymentRepository(session)

            payment = await repository.get_by_id(payment_id)

            if payment is None:
                raise PermanentEventError(f"Payment {payment_id} was not found")

            if payment.status != PaymentStatus.PENDING:
                return None

            return ProviderPaymentRequest(
                payment_id=payment.id,
                amount=payment.amount,
                currency=Currency(payment.currency),
            )

    async def _mark_unknown(self, payment_id: UUID) -> None:
        async with self._session_factory() as session:
            repository = PaymentRepository(session)

            try:
                payment = await repository.get_by_id_for_update(payment_id)

                if payment is None:
                    raise PermanentEventError(f"Payment {payment_id} was not found")

                if payment.status != PaymentStatus.PENDING:
                    return

                payment.status = PaymentStatus.UNKNOWN

                await session.commit()
            except Exception:
                await session.rollback()
                raise

    async def _apply_provider_result(
        self, *, payment_id: UUID, provider_response: ProviderPaymentResponse
    ) -> None:
        async with self._session_factory() as session:
            payment_repository = PaymentRepository(session)
            outbox_repository = OutboxRepository(session)

            try:
                payment = await payment_repository.get_by_id_for_update(payment_id)

                if payment is None:
                    raise PermanentEventError(f"Payment {payment_id} was not found")

                if payment.status != PaymentStatus.PENDING:
                    return

                processed_at = datetime.now(UTC)

                if provider_response.status == ProviderPaymentStatus.APPROVED:
                    payment.status = PaymentStatus.SUCCEEDED
                else:
                    payment.status = PaymentStatus.FAILED

                payment.provider_payment_id = provider_response.provider_payment_id
                payment.processed_at = processed_at

                webhook_event_id = uuid4()

                payment.webhook_event_id = webhook_event_id

                webhook_event = EventEnvelope(
                    event_id=webhook_event_id,
                    event_type=EventType.WEBHOOK_DELIVERY_REQUESTED,
                    payment_id=payment.id,
                    attempt=1,
                    occurred_at=processed_at,
                )

                await outbox_repository.add(
                    OutboxEvent(
                        id=webhook_event.event_id,
                        event_type=webhook_event.event_type.value,
                        payment_id=payment.id,
                        payload=webhook_event.model_dump(
                            mode="json",
                        ),
                        created_at=processed_at,
                    )
                )

                await session.commit()
            except Exception:
                await session.rollback()
                raise
