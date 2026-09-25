from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID, uuid4

import pytest
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.clients.payment_provider import PaymentProviderClient
from app.core.config import get_settings
from app.models.outbox_event import OutboxEvent
from app.models.payment import Payment, PaymentStatus
from app.repositories.outbox import OutboxRepository
from app.repositories.payment import PaymentRepository
from app.resilience.circuit_breaker import CircuitBreaker
from app.schemas.event import EventType
from app.schemas.provider import (
    ProviderPaymentResponse,
    ProviderPaymentStatus,
)
from app.services.payment_reconciliation import (
    PaymentReconciliationWorker,
)


def make_unknown_payment() -> Payment:
    payment = Payment(
        id=uuid4(),
        amount=Decimal("750.25"),
        currency="RUB",
        description="reconciliation integration test",
        payment_metadata={
            "order_id": "reconciliation-order",
        },
        status=PaymentStatus.UNKNOWN,
        idempotency_key=f"reconciliation-{uuid4()}",
        request_fingerprint=uuid4().hex,
        webhook_url="https://example.com/webhook",
        reconciliation_attempts=0,
        requires_manual_review=False,
    )

    payment.updated_at = datetime.now(UTC) - timedelta(minutes=10)

    return payment


def make_worker(
    session_factory: async_sessionmaker[AsyncSession],
    provider_client: PaymentProviderClient,
) -> PaymentReconciliationWorker:
    return PaymentReconciliationWorker(
        session_factory=session_factory,
        provider_client=provider_client,
        circuit_breaker=CircuitBreaker(
            failure_threshold=3,
            recovery_timeout_seconds=30.0,
        ),
        batch_size=10,
        stale_after_seconds=30.0,
        poll_interval_seconds=1.0,
        lease_seconds=60.0,
        max_attempts=5,
    )


@pytest.mark.integration
async def test_reconciliation_persists_terminal_result_and_webhook_outbox() -> None:
    settings = get_settings()

    engine = create_async_engine(
        str(settings.database_url),
        pool_pre_ping=True,
    )
    session_factory = async_sessionmaker(
        bind=engine,
        expire_on_commit=False,
    )

    payment = make_unknown_payment()
    provider_payment_id = uuid4()

    provider_client_mock = MagicMock(
        spec=PaymentProviderClient,
    )
    provider_client_mock.get_payment_by_idempotency_key = AsyncMock(
        return_value=ProviderPaymentResponse(
            provider_payment_id=provider_payment_id,
            status=ProviderPaymentStatus.APPROVED,
        )
    )

    worker = make_worker(
        session_factory,
        cast(
            PaymentProviderClient,
            provider_client_mock,
        ),
    )

    try:
        async with session_factory() as session:
            session.add(payment)
            await session.commit()

        reconciled_count = await worker.process_batch()

        assert reconciled_count == 1

        async with session_factory() as session:
            stored_payment = await session.get(
                Payment,
                payment.id,
            )

            assert stored_payment is not None
            assert stored_payment.status == PaymentStatus.SUCCEEDED
            assert stored_payment.provider_payment_id == provider_payment_id
            assert stored_payment.processed_at is not None
            assert stored_payment.webhook_event_id is not None

            assert stored_payment.reconciliation_lease_token is None
            assert stored_payment.reconciliation_lease_until is None

            result = await session.execute(
                select(OutboxEvent).where(
                    OutboxEvent.payment_id == payment.id,
                    OutboxEvent.event_type == EventType.WEBHOOK_DELIVERY_REQUESTED.value,
                )
            )

            events = list(
                result.scalars().all(),
            )

            assert len(events) == 1

            event = events[0]

            assert event.id == stored_payment.webhook_event_id
            assert event.payload["attempt"] == 1
            assert event.payload["payment_id"] == str(payment.id)
    finally:
        async with session_factory() as session:
            await session.execute(
                delete(OutboxEvent).where(
                    OutboxEvent.payment_id == payment.id,
                )
            )
            await session.execute(
                delete(Payment).where(
                    Payment.id == payment.id,
                )
            )
            await session.commit()

        await engine.dispose()


@pytest.mark.integration
async def test_reconciliation_rolls_back_terminal_transition_when_outbox_fails() -> None:
    settings = get_settings()

    engine = create_async_engine(
        str(settings.database_url),
        pool_pre_ping=True,
    )
    session_factory = async_sessionmaker(
        bind=engine,
        expire_on_commit=False,
    )

    payment = make_unknown_payment()

    provider_client_mock = MagicMock(
        spec=PaymentProviderClient,
    )
    provider_client_mock.get_payment_by_idempotency_key = AsyncMock(
        return_value=ProviderPaymentResponse(
            provider_payment_id=uuid4(),
            status=ProviderPaymentStatus.APPROVED,
        )
    )

    worker = make_worker(
        session_factory,
        cast(
            PaymentProviderClient,
            provider_client_mock,
        ),
    )

    async def fail_outbox_insert(
        _repository: OutboxRepository,
        _event: OutboxEvent,
    ) -> OutboxEvent:
        raise RuntimeError(
            "simulated reconciliation outbox failure",
        )

    try:
        async with session_factory() as session:
            session.add(payment)
            await session.commit()

        with patch.object(
            OutboxRepository,
            "add",
            fail_outbox_insert,
        ):
            reconciled_count = await worker.process_batch()

        assert reconciled_count == 0

        async with session_factory() as session:
            stored_payment = await session.get(
                Payment,
                payment.id,
            )

            assert stored_payment is not None
            assert stored_payment.status == PaymentStatus.UNKNOWN
            assert stored_payment.provider_payment_id is None
            assert stored_payment.processed_at is None
            assert stored_payment.webhook_event_id is None

            result = await session.execute(
                select(OutboxEvent).where(
                    OutboxEvent.payment_id == payment.id,
                )
            )

            assert result.scalar_one_or_none() is None
    finally:
        async with session_factory() as session:
            await session.execute(
                delete(OutboxEvent).where(
                    OutboxEvent.payment_id == payment.id,
                )
            )
            await session.execute(
                delete(Payment).where(
                    Payment.id == payment.id,
                )
            )
            await session.commit()

        await engine.dispose()


@pytest.mark.integration
async def test_stale_worker_cannot_persist_after_payment_is_reclaimed() -> None:
    settings = get_settings()

    engine = create_async_engine(
        str(settings.database_url),
        pool_pre_ping=True,
    )
    session_factory = async_sessionmaker(
        bind=engine,
        expire_on_commit=False,
    )

    payment = make_unknown_payment()

    worker_b_token = uuid4()

    provider_client_mock = MagicMock(
        spec=PaymentProviderClient,
    )

    async def lookup_and_reclaim(
        payment_id: UUID,
    ) -> ProviderPaymentResponse:
        async with session_factory() as session:
            stored_payment = await PaymentRepository(
                session,
            ).get_by_id(
                payment_id,
            )

            assert stored_payment is not None

            stored_payment.reconciliation_lease_until = datetime.now(UTC) - timedelta(seconds=1)

            await session.commit()

        reclaim_now = datetime.now(UTC)

        async with session_factory() as session:
            claimed_by_worker_b = await PaymentRepository(
                session,
            ).claim_reconciliation_batch(
                stale_before=(reclaim_now + timedelta(days=1)),
                lease_expired_before=reclaim_now,
                lease_token=worker_b_token,
                lease_until=(reclaim_now + timedelta(minutes=1)),
                batch_size=1,
            )

            await session.commit()

        assert claimed_by_worker_b == [
            payment_id,
        ]

        return ProviderPaymentResponse(
            provider_payment_id=uuid4(),
            status=ProviderPaymentStatus.APPROVED,
        )

    provider_client_mock.get_payment_by_idempotency_key = AsyncMock(
        side_effect=lookup_and_reclaim,
    )

    worker = make_worker(
        session_factory,
        cast(
            PaymentProviderClient,
            provider_client_mock,
        ),
    )

    try:
        async with session_factory() as session:
            session.add(payment)
            await session.commit()

        reconciled_count = await worker.process_batch()

        assert reconciled_count == 0

        async with session_factory() as session:
            stored_payment = await session.get(
                Payment,
                payment.id,
            )

            assert stored_payment is not None

            assert stored_payment.status == PaymentStatus.UNKNOWN
            assert stored_payment.reconciliation_lease_token == worker_b_token
            assert stored_payment.provider_payment_id is None
            assert stored_payment.webhook_event_id is None

            result = await session.execute(
                select(OutboxEvent).where(
                    OutboxEvent.payment_id == payment.id,
                )
            )

            assert result.scalar_one_or_none() is None
    finally:
        async with session_factory() as session:
            await session.execute(
                delete(OutboxEvent).where(
                    OutboxEvent.payment_id == payment.id,
                )
            )
            await session.execute(
                delete(Payment).where(
                    Payment.id == payment.id,
                )
            )
            await session.commit()

        await engine.dispose()
