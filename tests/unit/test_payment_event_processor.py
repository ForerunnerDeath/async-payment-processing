from datetime import UTC, datetime
from decimal import Decimal
from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID, uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.clients.payment_provider import (
    PaymentProviderClient,
    ProviderAmbiguousOutcomeError,
    ProviderPermanentError,
    ProviderUnavailableError,
)
from app.models.payment import Payment, PaymentStatus
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
from app.worker.consumer import (
    PermanentEventError,
    RetryableEventError,
)
from app.worker.payment_processor import PaymentEventProcessor

PAYMENT_ID = UUID(
    "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
)


def make_event(
    *,
    event_type: EventType = EventType.PAYMENT_PROCESS_REQUESTED,
) -> EventEnvelope:
    return EventEnvelope(
        event_id=uuid4(),
        event_type=event_type,
        payment_id=PAYMENT_ID,
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


def make_payment(
    *,
    status: PaymentStatus = PaymentStatus.PENDING,
) -> Payment:
    return Payment(
        id=PAYMENT_ID,
        amount=Decimal("500.15"),
        currency="RUB",
        description="test payment",
        payment_metadata={},
        status=status,
        idempotency_key="client-key",
        request_fingerprint="a" * 64,
        webhook_url="https://example.com/webhook",
    )


def make_session_factory(
    session: AsyncSession,
) -> async_sessionmaker[AsyncSession]:
    session_context = AsyncMock()
    session_context.__aenter__.return_value = session

    factory = MagicMock(
        return_value=session_context,
    )

    return cast(
        async_sessionmaker[AsyncSession],
        factory,
    )


async def test_approved_payment_becomes_succeeded_and_creates_webhook_outbox() -> None:
    payment = make_payment()

    session = AsyncMock(spec=AsyncSession)
    session_factory = make_session_factory(session)

    read_repository = MagicMock(spec=PaymentRepository)
    read_repository.get_by_id = AsyncMock(
        return_value=payment,
    )

    locked_repository = MagicMock(spec=PaymentRepository)
    locked_repository.get_by_id_for_update = AsyncMock(
        return_value=payment,
    )

    outbox_repository = MagicMock(spec=OutboxRepository)
    outbox_repository.add = AsyncMock()

    provider_client = MagicMock(spec=PaymentProviderClient)

    provider_response = ProviderPaymentResponse(
        provider_payment_id=uuid4(),
        status=ProviderPaymentStatus.APPROVED,
    )

    circuit_breaker = MagicMock(spec=CircuitBreaker)
    circuit_breaker.call = AsyncMock(
        return_value=provider_response,
    )

    processor = PaymentEventProcessor(
        session_factory=session_factory,
        provider_client=provider_client,
        circuit_breaker=circuit_breaker,
    )

    with (
        patch(
            "app.worker.payment_processor.PaymentRepository",
            side_effect=[
                read_repository,
                locked_repository,
            ],
        ),
        patch(
            "app.worker.payment_processor.OutboxRepository",
            return_value=outbox_repository,
        ),
    ):
        await processor.process(
            make_event(),
        )

    assert payment.status == PaymentStatus.SUCCEEDED
    assert payment.provider_payment_id == provider_response.provider_payment_id
    assert payment.processed_at is not None
    assert payment.webhook_event_id is not None

    outbox_repository.add.assert_awaited_once()

    outbox_event = outbox_repository.add.await_args.args[0]

    assert outbox_event.id == payment.webhook_event_id
    assert outbox_event.event_type == EventType.WEBHOOK_DELIVERY_REQUESTED.value
    assert outbox_event.payment_id == PAYMENT_ID

    assert outbox_event.payload["event_type"] == EventType.WEBHOOK_DELIVERY_REQUESTED.value
    assert outbox_event.payload["attempt"] == 1

    session.commit.assert_awaited_once_with()
    session.rollback.assert_not_awaited()


async def test_declined_payment_becomes_failed() -> None:
    payment = make_payment()

    session = AsyncMock(spec=AsyncSession)
    session_factory = make_session_factory(session)

    read_repository = MagicMock(spec=PaymentRepository)
    read_repository.get_by_id = AsyncMock(
        return_value=payment,
    )

    locked_repository = MagicMock(spec=PaymentRepository)
    locked_repository.get_by_id_for_update = AsyncMock(
        return_value=payment,
    )

    outbox_repository = MagicMock(spec=OutboxRepository)
    outbox_repository.add = AsyncMock()

    provider_client = MagicMock(spec=PaymentProviderClient)

    provider_response = ProviderPaymentResponse(
        provider_payment_id=uuid4(),
        status=ProviderPaymentStatus.DECLINED,
    )

    circuit_breaker = MagicMock(spec=CircuitBreaker)
    circuit_breaker.call = AsyncMock(
        return_value=provider_response,
    )

    processor = PaymentEventProcessor(
        session_factory,
        provider_client,
        circuit_breaker,
    )

    with (
        patch(
            "app.worker.payment_processor.PaymentRepository",
            side_effect=[
                read_repository,
                locked_repository,
            ],
        ),
        patch(
            "app.worker.payment_processor.OutboxRepository",
            return_value=outbox_repository,
        ),
    ):
        await processor.process(
            make_event(),
        )

    assert payment.status == PaymentStatus.FAILED
    assert payment.processed_at is not None
    assert payment.webhook_event_id is not None

    outbox_repository.add.assert_awaited_once()
    session.commit.assert_awaited_once_with()


@pytest.mark.parametrize(
    "payment_status",
    [
        PaymentStatus.SUCCEEDED,
        PaymentStatus.FAILED,
        PaymentStatus.UNKNOWN,
    ],
)
async def test_non_pending_payment_is_idempotent_noop(
    payment_status: PaymentStatus,
) -> None:
    payment = make_payment(
        status=payment_status,
    )

    session = AsyncMock(spec=AsyncSession)
    session_factory = make_session_factory(session)

    repository = MagicMock(spec=PaymentRepository)
    repository.get_by_id = AsyncMock(
        return_value=payment,
    )

    provider_client = MagicMock(spec=PaymentProviderClient)

    circuit_breaker = MagicMock(spec=CircuitBreaker)
    circuit_breaker.call = AsyncMock()

    processor = PaymentEventProcessor(
        session_factory,
        provider_client,
        circuit_breaker,
    )

    with patch(
        "app.worker.payment_processor.PaymentRepository",
        return_value=repository,
    ):
        await processor.process(
            make_event(),
        )

    circuit_breaker.call.assert_not_awaited()
    session.commit.assert_not_awaited()


async def test_provider_unavailable_becomes_retryable_event_error() -> None:
    payment = make_payment()

    session = AsyncMock(spec=AsyncSession)
    session_factory = make_session_factory(session)

    repository = MagicMock(spec=PaymentRepository)
    repository.get_by_id = AsyncMock(
        return_value=payment,
    )

    provider_client = MagicMock(spec=PaymentProviderClient)

    circuit_breaker = MagicMock(spec=CircuitBreaker)
    circuit_breaker.call = AsyncMock(
        side_effect=ProviderUnavailableError(
            "provider unavailable",
        ),
    )

    processor = PaymentEventProcessor(
        session_factory,
        provider_client,
        circuit_breaker,
    )

    with (
        patch(
            "app.worker.payment_processor.PaymentRepository",
            return_value=repository,
        ),
        pytest.raises(
            RetryableEventError,
            match="Payment provider is unavailable",
        ),
    ):
        await processor.process(
            make_event(),
        )

    assert payment.status == PaymentStatus.PENDING


async def test_open_circuit_breaker_becomes_retryable_event_error() -> None:
    payment = make_payment()

    session = AsyncMock(spec=AsyncSession)
    session_factory = make_session_factory(session)

    repository = MagicMock(spec=PaymentRepository)
    repository.get_by_id = AsyncMock(
        return_value=payment,
    )

    provider_client = MagicMock(spec=PaymentProviderClient)

    circuit_breaker = MagicMock(spec=CircuitBreaker)
    circuit_breaker.call = AsyncMock(
        side_effect=CircuitBreakerOpenError(
            "Circuit Breaker is open",
        ),
    )

    processor = PaymentEventProcessor(
        session_factory,
        provider_client,
        circuit_breaker,
    )

    with (
        patch(
            "app.worker.payment_processor.PaymentRepository",
            return_value=repository,
        ),
        pytest.raises(
            RetryableEventError,
            match="circuit breaker is open",
        ),
    ):
        await processor.process(
            make_event(),
        )


async def test_ambiguous_provider_outcome_marks_payment_unknown() -> None:
    payment = make_payment()

    session = AsyncMock(spec=AsyncSession)
    session_factory = make_session_factory(session)

    read_repository = MagicMock(spec=PaymentRepository)
    read_repository.get_by_id = AsyncMock(
        return_value=payment,
    )

    locked_repository = MagicMock(spec=PaymentRepository)
    locked_repository.get_by_id_for_update = AsyncMock(
        return_value=payment,
    )

    provider_client = MagicMock(spec=PaymentProviderClient)

    circuit_breaker = MagicMock(spec=CircuitBreaker)
    circuit_breaker.call = AsyncMock(
        side_effect=ProviderAmbiguousOutcomeError(
            "outcome unknown",
        ),
    )

    processor = PaymentEventProcessor(
        session_factory,
        provider_client,
        circuit_breaker,
    )

    with patch(
        "app.worker.payment_processor.PaymentRepository",
        side_effect=[
            read_repository,
            locked_repository,
        ],
    ):
        await processor.process(
            make_event(),
        )

    assert payment.status == PaymentStatus.UNKNOWN
    assert payment.processed_at is None
    assert payment.webhook_event_id is None

    session.commit.assert_awaited_once_with()


async def test_permanent_provider_error_becomes_permanent_event_error() -> None:
    payment = make_payment()

    session = AsyncMock(spec=AsyncSession)
    session_factory = make_session_factory(session)

    repository = MagicMock(spec=PaymentRepository)
    repository.get_by_id = AsyncMock(
        return_value=payment,
    )

    provider_client = MagicMock(spec=PaymentProviderClient)

    circuit_breaker = MagicMock(spec=CircuitBreaker)
    circuit_breaker.call = AsyncMock(
        side_effect=ProviderPermanentError(
            "rejected",
            status_code=400,
        ),
    )

    processor = PaymentEventProcessor(
        session_factory,
        provider_client,
        circuit_breaker,
    )

    with (
        patch(
            "app.worker.payment_processor.PaymentRepository",
            return_value=repository,
        ),
        pytest.raises(
            PermanentEventError,
            match="permanently rejected",
        ),
    ):
        await processor.process(
            make_event(),
        )

    assert payment.status == PaymentStatus.PENDING


async def test_missing_payment_is_permanent_event_error() -> None:
    session = AsyncMock(spec=AsyncSession)
    session_factory = make_session_factory(session)

    repository = MagicMock(spec=PaymentRepository)
    repository.get_by_id = AsyncMock(
        return_value=None,
    )

    provider_client = MagicMock(spec=PaymentProviderClient)
    circuit_breaker = MagicMock(spec=CircuitBreaker)

    processor = PaymentEventProcessor(
        session_factory,
        provider_client,
        circuit_breaker,
    )

    with (
        patch(
            "app.worker.payment_processor.PaymentRepository",
            return_value=repository,
        ),
        pytest.raises(
            PermanentEventError,
            match="was not found",
        ),
    ):
        await processor.process(
            make_event(),
        )


async def test_terminal_state_won_by_concurrent_processor_is_not_overwritten() -> None:
    payment_before_call = make_payment()

    payment_after_call = make_payment(
        status=PaymentStatus.SUCCEEDED,
    )
    payment_after_call.processed_at = None

    session = AsyncMock(spec=AsyncSession)
    session_factory = make_session_factory(session)

    read_repository = MagicMock(spec=PaymentRepository)
    read_repository.get_by_id = AsyncMock(
        return_value=payment_before_call,
    )

    locked_repository = MagicMock(spec=PaymentRepository)
    locked_repository.get_by_id_for_update = AsyncMock(
        return_value=payment_after_call,
    )

    outbox_repository = MagicMock(spec=OutboxRepository)
    outbox_repository.add = AsyncMock()

    provider_client = MagicMock(spec=PaymentProviderClient)

    circuit_breaker = MagicMock(spec=CircuitBreaker)
    circuit_breaker.call = AsyncMock(
        return_value=ProviderPaymentResponse(
            provider_payment_id=uuid4(),
            status=ProviderPaymentStatus.APPROVED,
        ),
    )

    processor = PaymentEventProcessor(
        session_factory,
        provider_client,
        circuit_breaker,
    )

    with (
        patch(
            "app.worker.payment_processor.PaymentRepository",
            side_effect=[
                read_repository,
                locked_repository,
            ],
        ),
        patch(
            "app.worker.payment_processor.OutboxRepository",
            return_value=outbox_repository,
        ),
    ):
        await processor.process(
            make_event(),
        )

    outbox_repository.add.assert_not_awaited()
    session.commit.assert_not_awaited()
