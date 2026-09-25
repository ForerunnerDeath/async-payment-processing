import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID, uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.clients.payment_provider import (
    PaymentProviderClient,
    ProviderUnavailableError,
)
from app.models.outbox_event import OutboxEvent
from app.models.payment import Payment, PaymentStatus
from app.repositories.outbox import OutboxRepository
from app.repositories.payment import PaymentRepository
from app.resilience.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerOpenError,
)
from app.schemas.event import EventType
from app.schemas.provider import (
    ProviderPaymentResponse,
    ProviderPaymentStatus,
)
from app.services.payment_reconciliation import (
    PaymentReconciliationWorker,
)


class ReconciliationWorkerHarness(
    PaymentReconciliationWorker,
):
    async def claim_batch_for_test(
        self,
    ) -> tuple[UUID, list[UUID]]:
        return await self._claim_batch()

    async def renew_claim_for_test(
        self,
        *,
        payment_id: UUID,
        lease_token: UUID,
    ) -> bool:
        return await self._renew_claim(
            payment_id=payment_id,
            lease_token=lease_token,
        )

    async def lookup_provider_for_test(
        self,
        payment_id: UUID,
    ) -> ProviderPaymentResponse | None:
        return await self._lookup_provider(
            payment_id,
        )

    async def persist_resolution_for_test(
        self,
        *,
        payment_id: UUID,
        lease_token: UUID,
        provider_response: ProviderPaymentResponse | None,
    ) -> bool:
        return await self._persist_resolution(
            payment_id=payment_id,
            lease_token=lease_token,
            provider_response=provider_response,
        )


def make_session_factory(
    session: AsyncSession,
) -> async_sessionmaker[AsyncSession]:
    session_context = MagicMock()
    session_context.__aenter__ = AsyncMock(
        return_value=session,
    )
    session_context.__aexit__ = AsyncMock(
        return_value=None,
    )

    factory = MagicMock(
        return_value=session_context,
    )

    return cast(
        async_sessionmaker[AsyncSession],
        factory,
    )


def make_worker(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    provider_client: PaymentProviderClient | None = None,
    circuit_breaker: CircuitBreaker | None = None,
    max_attempts: int = 5,
) -> ReconciliationWorkerHarness:
    if provider_client is None:
        provider_client = cast(
            PaymentProviderClient,
            MagicMock(spec=PaymentProviderClient),
        )

    if circuit_breaker is None:
        circuit_breaker = cast(
            CircuitBreaker,
            MagicMock(spec=CircuitBreaker),
        )

    return ReconciliationWorkerHarness(
        session_factory=session_factory,
        provider_client=provider_client,
        circuit_breaker=circuit_breaker,
        batch_size=20,
        stale_after_seconds=30.0,
        poll_interval_seconds=1.0,
        lease_seconds=60.0,
        max_attempts=max_attempts,
    )


def make_unknown_payment(
    *,
    reconciliation_attempts: int = 0,
) -> Payment:
    return Payment(
        id=uuid4(),
        amount=Decimal("500.15"),
        currency="RUB",
        description="reconciliation test",
        payment_metadata={},
        status=PaymentStatus.UNKNOWN,
        idempotency_key=f"reconciliation-{uuid4()}",
        request_fingerprint=uuid4().hex,
        webhook_url="https://example.com/webhook",
        reconciliation_attempts=reconciliation_attempts,
        requires_manual_review=False,
        reconciliation_lease_token=uuid4(),
        reconciliation_lease_until=(datetime.now(UTC) + timedelta(minutes=1)),
    )


async def test_claim_batch_commits_claim() -> None:
    session = AsyncMock(spec=AsyncSession)
    session_factory = make_session_factory(session)

    repository = MagicMock(spec=PaymentRepository)
    repository.claim_reconciliation_batch = AsyncMock(
        return_value=[
            uuid4(),
            uuid4(),
        ]
    )

    worker = make_worker(
        session_factory,
    )

    with patch(
        "app.services.payment_reconciliation.PaymentRepository",
        return_value=repository,
    ):
        lease_token, payment_ids = await worker.claim_batch_for_test()

    assert len(payment_ids) == 2
    assert lease_token is not None

    repository.claim_reconciliation_batch.assert_awaited_once()
    session.commit.assert_awaited_once_with()
    session.rollback.assert_not_awaited()


async def test_renew_claim_returns_false_when_lease_is_lost() -> None:
    session = AsyncMock(spec=AsyncSession)
    session_factory = make_session_factory(session)

    repository = MagicMock(spec=PaymentRepository)
    repository.renew_reconciliation_lease = AsyncMock(
        return_value=False,
    )

    worker = make_worker(
        session_factory,
    )

    with patch(
        "app.services.payment_reconciliation.PaymentRepository",
        return_value=repository,
    ):
        renewed = await worker.renew_claim_for_test(
            payment_id=uuid4(),
            lease_token=uuid4(),
        )

    assert renewed is False

    session.rollback.assert_awaited_once_with()
    session.commit.assert_not_awaited()


async def test_lookup_provider_returns_response() -> None:
    session = AsyncMock(spec=AsyncSession)

    provider_response = ProviderPaymentResponse(
        provider_payment_id=uuid4(),
        status=ProviderPaymentStatus.APPROVED,
    )

    provider_client = MagicMock(
        spec=PaymentProviderClient,
    )

    circuit_breaker = MagicMock(
        spec=CircuitBreaker,
    )
    circuit_breaker.call = AsyncMock(
        return_value=provider_response,
    )

    worker = make_worker(
        make_session_factory(session),
        provider_client=cast(
            PaymentProviderClient,
            provider_client,
        ),
        circuit_breaker=cast(
            CircuitBreaker,
            circuit_breaker,
        ),
    )

    payment_id = uuid4()

    result = await worker.lookup_provider_for_test(
        payment_id,
    )

    assert result == provider_response

    circuit_breaker.call.assert_awaited_once_with(
        provider_client.get_payment_by_idempotency_key,
        payment_id,
    )


@pytest.mark.parametrize(
    "error",
    [
        ProviderUnavailableError(
            "provider unavailable",
        ),
        CircuitBreakerOpenError(
            "circuit breaker is open",
        ),
    ],
)
async def test_lookup_failure_is_unresolved(
    error: Exception,
) -> None:
    session = AsyncMock(spec=AsyncSession)

    provider_client = MagicMock(
        spec=PaymentProviderClient,
    )

    circuit_breaker = MagicMock(
        spec=CircuitBreaker,
    )
    circuit_breaker.call = AsyncMock(
        side_effect=error,
    )

    worker = make_worker(
        make_session_factory(session),
        provider_client=cast(
            PaymentProviderClient,
            provider_client,
        ),
        circuit_breaker=cast(
            CircuitBreaker,
            circuit_breaker,
        ),
    )

    result = await worker.lookup_provider_for_test(
        uuid4(),
    )

    assert result is None


@pytest.mark.parametrize(
    ("provider_status", "expected_payment_status"),
    [
        (
            ProviderPaymentStatus.APPROVED,
            PaymentStatus.SUCCEEDED,
        ),
        (
            ProviderPaymentStatus.DECLINED,
            PaymentStatus.FAILED,
        ),
    ],
)
async def test_persist_terminal_provider_result(
    provider_status: ProviderPaymentStatus,
    expected_payment_status: PaymentStatus,
) -> None:
    session = AsyncMock(spec=AsyncSession)
    session_factory = make_session_factory(session)

    payment = make_unknown_payment()
    lease_token = payment.reconciliation_lease_token

    assert lease_token is not None

    payment_repository = MagicMock(
        spec=PaymentRepository,
    )
    payment_repository.get_claimed_payment_for_update = AsyncMock(
        return_value=payment,
    )

    added_events: list[OutboxEvent] = []

    async def add_outbox_event(
        event: OutboxEvent,
    ) -> OutboxEvent:
        added_events.append(event)
        return event

    outbox_repository = MagicMock(
        spec=OutboxRepository,
    )
    outbox_repository.add = AsyncMock(
        side_effect=add_outbox_event,
    )

    provider_payment_id = uuid4()

    worker = make_worker(
        session_factory,
    )

    with (
        patch(
            "app.services.payment_reconciliation.PaymentRepository",
            return_value=payment_repository,
        ),
        patch(
            "app.services.payment_reconciliation.OutboxRepository",
            return_value=outbox_repository,
        ),
    ):
        reconciled = await worker.persist_resolution_for_test(
            payment_id=payment.id,
            lease_token=lease_token,
            provider_response=ProviderPaymentResponse(
                provider_payment_id=provider_payment_id,
                status=provider_status,
            ),
        )

    assert reconciled is True

    assert payment.status == expected_payment_status
    assert payment.provider_payment_id == provider_payment_id
    assert payment.processed_at is not None
    assert payment.webhook_event_id is not None

    assert payment.reconciliation_lease_token is None
    assert payment.reconciliation_lease_until is None

    assert len(added_events) == 1

    outbox_event = added_events[0]

    assert outbox_event.event_type == EventType.WEBHOOK_DELIVERY_REQUESTED.value
    assert outbox_event.payment_id == payment.id
    assert outbox_event.id == payment.webhook_event_id
    assert outbox_event.payload["attempt"] == 1

    session.commit.assert_awaited_once_with()
    session.rollback.assert_not_awaited()


async def test_unresolved_lookup_increments_attempt_and_releases_lease() -> None:
    session = AsyncMock(spec=AsyncSession)
    session_factory = make_session_factory(session)

    payment = make_unknown_payment(
        reconciliation_attempts=1,
    )

    lease_token = payment.reconciliation_lease_token

    assert lease_token is not None

    repository = MagicMock(
        spec=PaymentRepository,
    )
    repository.get_claimed_payment_for_update = AsyncMock(
        return_value=payment,
    )

    worker = make_worker(
        session_factory,
        max_attempts=5,
    )

    with patch(
        "app.services.payment_reconciliation.PaymentRepository",
        return_value=repository,
    ):
        reconciled = await worker.persist_resolution_for_test(
            payment_id=payment.id,
            lease_token=lease_token,
            provider_response=None,
        )

    assert reconciled is False
    assert payment.status == PaymentStatus.UNKNOWN

    assert payment.reconciliation_attempts == 2
    assert payment.requires_manual_review is False

    assert payment.reconciliation_lease_token is None
    assert payment.reconciliation_lease_until is None

    session.commit.assert_awaited_once_with()


async def test_max_attempts_requires_manual_review() -> None:
    session = AsyncMock(spec=AsyncSession)
    session_factory = make_session_factory(session)

    payment = make_unknown_payment(
        reconciliation_attempts=4,
    )

    lease_token = payment.reconciliation_lease_token

    assert lease_token is not None

    repository = MagicMock(
        spec=PaymentRepository,
    )
    repository.get_claimed_payment_for_update = AsyncMock(
        return_value=payment,
    )

    worker = make_worker(
        session_factory,
        max_attempts=5,
    )

    with patch(
        "app.services.payment_reconciliation.PaymentRepository",
        return_value=repository,
    ):
        reconciled = await worker.persist_resolution_for_test(
            payment_id=payment.id,
            lease_token=lease_token,
            provider_response=None,
        )

    assert reconciled is False
    assert payment.reconciliation_attempts == 5
    assert payment.requires_manual_review is True

    assert payment.reconciliation_lease_token is None
    assert payment.reconciliation_lease_until is None

    session.commit.assert_awaited_once_with()


async def test_stale_worker_cannot_persist_resolution() -> None:
    session = AsyncMock(spec=AsyncSession)
    session_factory = make_session_factory(session)

    repository = MagicMock(
        spec=PaymentRepository,
    )
    repository.get_claimed_payment_for_update = AsyncMock(
        return_value=None,
    )

    outbox_repository = MagicMock(
        spec=OutboxRepository,
    )
    outbox_repository.add = AsyncMock()

    worker = make_worker(
        session_factory,
    )

    with (
        patch(
            "app.services.payment_reconciliation.PaymentRepository",
            return_value=repository,
        ),
        patch(
            "app.services.payment_reconciliation.OutboxRepository",
            return_value=outbox_repository,
        ),
    ):
        reconciled = await worker.persist_resolution_for_test(
            payment_id=uuid4(),
            lease_token=uuid4(),
            provider_response=ProviderPaymentResponse(
                provider_payment_id=uuid4(),
                status=ProviderPaymentStatus.APPROVED,
            ),
        )

    assert reconciled is False

    outbox_repository.add.assert_not_awaited()

    session.rollback.assert_awaited_once_with()
    session.commit.assert_not_awaited()


async def test_run_stops_when_stop_event_is_set() -> None:
    worker = make_worker(
        make_session_factory(
            AsyncMock(spec=AsyncSession),
        )
    )

    stop_event = asyncio.Event()

    process_batch = AsyncMock()

    async def process_once() -> int:
        stop_event.set()
        return 0

    process_batch.side_effect = process_once

    with patch.object(
        worker,
        "process_batch",
        process_batch,
    ):
        await worker.run(
            stop_event,
        )

    process_batch.assert_awaited_once_with()
