from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.outbox_event import OutboxEvent
from app.models.payment import Payment, PaymentStatus
from app.repositories.outbox import OutboxRepository
from app.repositories.payment import PaymentRepository
from app.schemas.event import EventEnvelope, EventType
from app.schemas.payment import PaymentCreate
from app.services.idempotency import build_request_fingerprint


class IdempotencyConflictError(Exception):
    pass


@dataclass(frozen=True, slots=True)
class PaymentCreationResult:
    payment: Payment
    created: bool


class PaymentService:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self._payment_repository = PaymentRepository(session)
        self._outbox_repository = OutboxRepository(session)

    async def create_payment(
        self, data: PaymentCreate, *, idempotency_key: str
    ) -> PaymentCreationResult:
        request_fingerprint = build_request_fingerprint(data)

        existing_payment = await self._payment_repository.get_by_idempotency_key(
            idempotency_key,
        )

        if existing_payment is not None:
            return self._resolve_existing_payment(
                existing_payment,
                request_fingerprint=request_fingerprint,
            )

        payment = Payment(
            amount=data.amount,
            currency=data.currency.value,
            description=data.description,
            payment_metadata=dict(data.metadata),
            status=PaymentStatus.PENDING,
            idempotency_key=idempotency_key,
            request_fingerprint=request_fingerprint,
            webhook_url=str(data.webhook_url),
        )

        try:
            payment = await self._payment_repository.add(payment)

            outbox_event = self._build_process_requested_event(payment)

            await self._outbox_repository.add(outbox_event)

            await self._session.commit()
        except IntegrityError:
            await self._session.rollback()

            existing_payment = await self._payment_repository.get_by_idempotency_key(
                idempotency_key,
            )

            if existing_payment is None:
                raise

            return self._resolve_existing_payment(
                existing_payment,
                request_fingerprint=request_fingerprint,
            )
        except Exception:
            await self._session.rollback()
            raise

        await self._session.refresh(payment)

        return PaymentCreationResult(
            payment=payment,
            created=True,
        )

    @staticmethod
    def _resolve_existing_payment(
        payment: Payment, *, request_fingerprint: str
    ) -> PaymentCreationResult:
        if payment.request_fingerprint != request_fingerprint:
            raise IdempotencyConflictError(
                "Idempotency-Key was already used with a different request",
            )

        return PaymentCreationResult(
            payment=payment,
            created=False,
        )

    @staticmethod
    def _build_process_requested_event(
        payment: Payment,
    ) -> OutboxEvent:
        event_id = uuid4()
        occurred_at = datetime.now(UTC)

        envelope = EventEnvelope(
            event_id=event_id,
            event_type=EventType.PAYMENT_PROCESS_REQUESTED,
            payment_id=payment.id,
            occurred_at=occurred_at,
        )

        return OutboxEvent(
            id=event_id,
            event_type=envelope.event_type.value,
            payment_id=payment.id,
            payload=envelope.model_dump(mode="json"),
            created_at=occurred_at,
        )
