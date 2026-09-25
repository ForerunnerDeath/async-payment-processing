import asyncio
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import structlog
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
from app.schemas.event import EventEnvelope, EventType
from app.schemas.provider import (
    ProviderPaymentResponse,
    ProviderPaymentStatus,
)

logger = structlog.get_logger()


class PaymentReconciliationWorker:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        provider_client: PaymentProviderClient,
        circuit_breaker: CircuitBreaker,
        *,
        batch_size: int,
        stale_after_seconds: float,
        poll_interval_seconds: float,
        lease_seconds: float,
        max_attempts: int,
    ) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be at least 1")

        if stale_after_seconds < 0:
            raise ValueError("stale_after_seconds must not be negative")

        if poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be greater than zero")

        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be greater than zero")

        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")

        self._session_factory = session_factory
        self._provider_client = provider_client
        self._circuit_breaker = circuit_breaker

        self._batch_size = batch_size
        self._stale_after_seconds = stale_after_seconds
        self._poll_interval_seconds = poll_interval_seconds
        self._lease_seconds = lease_seconds
        self._max_attempts = max_attempts

    async def process_batch(self) -> int:
        lease_token, payment_ids = await self._claim_batch()

        reconciled_count = 0

        for payment_id in payment_ids:
            try:
                lease_valid = await self._renew_claim(
                    payment_id=payment_id,
                    lease_token=lease_token,
                )

                if not lease_valid:
                    continue

                provider_response = await self._lookup_provider(payment_id)

                reconciled = await self._persist_resolution(
                    payment_id=payment_id,
                    lease_token=lease_token,
                    provider_response=provider_response,
                )

                if reconciled:
                    reconciled_count += 1

            except Exception:
                logger.exception(
                    "payment_reconciliation_payment_failed",
                    payment_id=str(payment_id),
                )

        return reconciled_count

    async def run(self, stop_event: asyncio.Event) -> None:
        logger.info("payment_reconciliation_started")

        try:
            while not stop_event.is_set():
                try:
                    reconciled_count = await self.process_batch()

                    if reconciled_count > 0:
                        logger.info(
                            "payment_reconciliation_batch_processed",
                            count=reconciled_count,
                        )

                except Exception:
                    logger.exception("payment_reconciliation_batch_failed")

                with suppress(TimeoutError):
                    await asyncio.wait_for(
                        stop_event.wait(),
                        timeout=self._poll_interval_seconds,
                    )
        finally:
            logger.info("payment_reconciliation_stopped")

    async def _claim_batch(self) -> tuple[UUID, list[UUID]]:
        now = datetime.now(UTC)

        stale_before = now - timedelta(seconds=self._stale_after_seconds)

        lease_until = now + timedelta(seconds=self._lease_seconds)

        lease_token = uuid4()

        async with self._session_factory() as session:
            repository = PaymentRepository(session)

            try:
                payment_ids = await repository.claim_reconciliation_batch(
                    stale_before=stale_before,
                    lease_expired_before=now,
                    lease_token=lease_token,
                    lease_until=lease_until,
                    batch_size=self._batch_size,
                )

                await session.commit()

                return lease_token, payment_ids

            except Exception:
                await session.rollback()
                raise

    async def _renew_claim(self, *, payment_id: UUID, lease_token: UUID) -> bool:
        now = datetime.now(UTC)

        lease_until = now + timedelta(seconds=self._lease_seconds)

        async with self._session_factory() as session:
            repository = PaymentRepository(session)

            try:
                renewed = await repository.renew_reconciliation_lease(
                    payment_id=payment_id,
                    lease_token=lease_token,
                    now=now,
                    lease_until=lease_until,
                )

                if not renewed:
                    await session.rollback()

                    logger.warning(
                        "payment_reconciliation_lease_lost",
                        payment_id=str(payment_id),
                    )

                    return False

                await session.commit()

                return True

            except Exception:
                await session.rollback()
                raise

    async def _lookup_provider(self, payment_id: UUID) -> ProviderPaymentResponse | None:
        try:
            response = await self._circuit_breaker.call(
                self._provider_client.get_payment_by_idempotency_key,
                payment_id,
            )

        except (
            CircuitBreakerOpenError,
            ProviderUnavailableError,
            ProviderAmbiguousOutcomeError,
            ProviderPermanentError,
        ) as exc:
            logger.warning(
                "payment_reconciliation_lookup_failed",
                payment_id=str(payment_id),
                error_type=type(exc).__name__,
            )

            return None

        if response is None:
            logger.warning(
                "payment_reconciliation_provider_payment_not_found",
                payment_id=str(payment_id),
            )

        return response

    async def _persist_resolution(
        self,
        *,
        payment_id: UUID,
        lease_token: UUID,
        provider_response: ProviderPaymentResponse | None,
    ) -> bool:
        now = datetime.now(UTC)

        async with self._session_factory() as session:
            payment_repository = PaymentRepository(session)

            try:
                payment = await payment_repository.get_claimed_payment_for_update(
                    payment_id=payment_id,
                    lease_token=lease_token,
                    now=now,
                )

                if payment is None:
                    await session.rollback()

                    logger.warning(
                        "payment_reconciliation_lease_lost_before_persist",
                        payment_id=str(payment_id),
                    )

                    return False

                if provider_response is None:
                    payment.reconciliation_attempts += 1

                    if payment.reconciliation_attempts >= self._max_attempts:
                        payment.requires_manual_review = True

                        logger.warning(
                            "payment_reconciliation_manual_review_required",
                            payment_id=str(payment.id),
                            attempts=payment.reconciliation_attempts,
                        )

                    payment.reconciliation_lease_token = None
                    payment.reconciliation_lease_until = None

                    await session.commit()

                    return False

                processed_at = datetime.now(UTC)

                if provider_response.status == ProviderPaymentStatus.APPROVED:
                    payment.status = PaymentStatus.SUCCEEDED
                else:
                    payment.status = PaymentStatus.FAILED

                payment.provider_payment_id = provider_response.provider_payment_id
                payment.processed_at = processed_at

                webhook_event_id = uuid4()

                payment.webhook_event_id = webhook_event_id

                payment.reconciliation_lease_token = None
                payment.reconciliation_lease_until = None

                webhook_event = EventEnvelope(
                    event_id=webhook_event_id,
                    event_type=(EventType.WEBHOOK_DELIVERY_REQUESTED),
                    payment_id=payment.id,
                    attempt=1,
                    occurred_at=processed_at,
                )

                outbox_event = OutboxEvent(
                    id=webhook_event.event_id,
                    event_type=webhook_event.event_type.value,
                    payment_id=payment.id,
                    payload=webhook_event.model_dump(
                        mode="json",
                    ),
                )

                await OutboxRepository(
                    session,
                ).add(
                    outbox_event,
                )

                await session.commit()

                return True

            except Exception:
                await session.rollback()
                raise
