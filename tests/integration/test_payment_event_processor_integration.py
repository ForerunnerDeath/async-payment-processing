from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import (
    async_sessionmaker,
    create_async_engine,
)

from app.clients.payment_provider import PaymentProviderClient
from app.core.config import get_settings
from app.models.outbox_event import OutboxEvent
from app.models.payment import Payment, PaymentStatus
from app.repositories.outbox import OutboxRepository
from app.resilience.circuit_breaker import CircuitBreaker
from app.schemas.event import EventEnvelope, EventType
from app.schemas.provider import (
    ProviderPaymentResponse,
    ProviderPaymentStatus,
)
from app.worker.payment_processor import PaymentEventProcessor


def make_payment() -> Payment:
    return Payment(
        id=uuid4(),
        amount=Decimal("500.15"),
        currency="RUB",
        description="integration payment",
        payment_metadata={
            "source": "integration-test",
        },
        status=PaymentStatus.PENDING,
        idempotency_key=f"integration-{uuid4()}",
        request_fingerprint="a" * 64,
        webhook_url="https://example.com/webhook",
    )


def make_event(
    payment: Payment,
) -> EventEnvelope:
    return EventEnvelope(
        event_id=uuid4(),
        event_type=EventType.PAYMENT_PROCESS_REQUESTED,
        payment_id=payment.id,
        attempt=1,
        occurred_at=datetime.now(UTC),
    )


@pytest.mark.integration
async def test_approved_payment_and_webhook_outbox_are_committed_atomically() -> None:
    settings = get_settings()

    engine = create_async_engine(
        str(settings.database_url),
        pool_pre_ping=True,
    )
    session_factory = async_sessionmaker(
        bind=engine,
        expire_on_commit=False,
    )

    payment = make_payment()
    event = make_event(payment)

    provider_payment_id = uuid4()

    provider_client = MagicMock(
        spec=PaymentProviderClient,
    )

    circuit_breaker = MagicMock(
        spec=CircuitBreaker,
    )
    circuit_breaker.call = AsyncMock(
        return_value=ProviderPaymentResponse(
            provider_payment_id=provider_payment_id,
            status=ProviderPaymentStatus.APPROVED,
        )
    )

    processor = PaymentEventProcessor(
        session_factory=session_factory,
        provider_client=provider_client,
        circuit_breaker=circuit_breaker,
    )

    try:
        async with session_factory() as session:
            session.add(payment)
            await session.commit()

        await processor.process(event)

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

            result = await session.execute(
                select(OutboxEvent).where(
                    OutboxEvent.payment_id == payment.id,
                    OutboxEvent.event_type == EventType.WEBHOOK_DELIVERY_REQUESTED.value,
                )
            )

            webhook_events = list(
                result.scalars().all(),
            )

            assert len(webhook_events) == 1

            webhook_event = webhook_events[0]

            assert webhook_event.id == stored_payment.webhook_event_id
            assert webhook_event.payload["payment_id"] == str(payment.id)
            assert webhook_event.payload["event_type"] == EventType.WEBHOOK_DELIVERY_REQUESTED.value
            assert webhook_event.payload["attempt"] == 1
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
async def test_payment_transition_is_rolled_back_when_webhook_outbox_insert_fails() -> None:
    settings = get_settings()

    engine = create_async_engine(
        str(settings.database_url),
        pool_pre_ping=True,
    )
    session_factory = async_sessionmaker(
        bind=engine,
        expire_on_commit=False,
    )

    payment = make_payment()
    event = make_event(payment)

    provider_client = MagicMock(
        spec=PaymentProviderClient,
    )

    circuit_breaker = MagicMock(
        spec=CircuitBreaker,
    )
    circuit_breaker.call = AsyncMock(
        return_value=ProviderPaymentResponse(
            provider_payment_id=uuid4(),
            status=ProviderPaymentStatus.APPROVED,
        )
    )

    processor = PaymentEventProcessor(
        session_factory=session_factory,
        provider_client=provider_client,
        circuit_breaker=circuit_breaker,
    )

    original_add = OutboxRepository.add

    async def fail_webhook_outbox_insert(
        repository: OutboxRepository,
        outbox_event: OutboxEvent,
    ) -> OutboxEvent:
        if outbox_event.event_type == EventType.WEBHOOK_DELIVERY_REQUESTED.value:
            raise RuntimeError(
                "simulated outbox failure",
            )

        return await original_add(
            repository,
            outbox_event,
        )

    try:
        async with session_factory() as session:
            session.add(payment)
            await session.commit()

        with (
            patch.object(
                OutboxRepository,
                "add",
                fail_webhook_outbox_insert,
            ),
            pytest.raises(
                RuntimeError,
                match="simulated outbox failure",
            ),
        ):
            await processor.process(event)

        async with session_factory() as session:
            stored_payment = await session.get(
                Payment,
                payment.id,
            )

            assert stored_payment is not None

            assert stored_payment.status == PaymentStatus.PENDING
            assert stored_payment.provider_payment_id is None
            assert stored_payment.processed_at is None
            assert stored_payment.webhook_event_id is None

            result = await session.execute(
                select(OutboxEvent).where(
                    OutboxEvent.payment_id == payment.id,
                    OutboxEvent.event_type == EventType.WEBHOOK_DELIVERY_REQUESTED.value,
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
