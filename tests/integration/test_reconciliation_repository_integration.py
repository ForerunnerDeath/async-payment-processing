from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import (
    async_sessionmaker,
    create_async_engine,
)

from app.core.config import get_settings
from app.models.payment import Payment, PaymentStatus
from app.repositories.payment import PaymentRepository


def make_unknown_payment(
    *,
    updated_at: datetime,
    requires_manual_review: bool = False,
) -> Payment:
    payment = Payment(
        id=uuid4(),
        amount=Decimal("100.00"),
        currency="RUB",
        description="reconciliation integration test",
        payment_metadata={},
        status=PaymentStatus.UNKNOWN,
        idempotency_key=f"reconciliation-{uuid4()}",
        request_fingerprint=uuid4().hex,
        webhook_url="https://example.com/webhook",
        requires_manual_review=requires_manual_review,
    )

    payment.updated_at = updated_at

    return payment


@pytest.mark.integration
async def test_two_workers_claim_disjoint_batches() -> None:
    settings = get_settings()

    engine = create_async_engine(
        str(settings.database_url),
        pool_pre_ping=True,
    )
    session_factory = async_sessionmaker(
        bind=engine,
        expire_on_commit=False,
    )

    stale_at = datetime.now(UTC) - timedelta(minutes=10)

    payments = [
        make_unknown_payment(
            updated_at=stale_at,
        )
        for _ in range(4)
    ]

    payment_ids = {payment.id for payment in payments}

    try:
        async with session_factory() as session:
            session.add_all(payments)
            await session.commit()

        now = datetime.now(UTC)

        token_a = uuid4()
        token_b = uuid4()

        lease_until = now + timedelta(minutes=1)

        async with (
            session_factory() as session_a,
            session_factory() as session_b,
        ):
            repository_a = PaymentRepository(
                session_a,
            )
            repository_b = PaymentRepository(
                session_b,
            )

            claimed_by_a = await repository_a.claim_reconciliation_batch(
                stale_before=now,
                lease_expired_before=now,
                lease_token=token_a,
                lease_until=lease_until,
                batch_size=2,
            )

            # session_a ещё не commit.
            # Эти две строки остаются под FOR UPDATE lock.

            claimed_by_b = await repository_b.claim_reconciliation_batch(
                stale_before=now,
                lease_expired_before=now,
                lease_token=token_b,
                lease_until=lease_until,
                batch_size=2,
            )

            await session_b.commit()
            await session_a.commit()

        assert len(claimed_by_a) == 2
        assert len(claimed_by_b) == 2

        assert set(claimed_by_a).isdisjoint(
            claimed_by_b,
        )

        assert set(claimed_by_a) | set(claimed_by_b) == payment_ids
    finally:
        async with session_factory() as session:
            await session.execute(
                delete(Payment).where(
                    Payment.id.in_(payment_ids),
                )
            )
            await session.commit()

        await engine.dispose()


@pytest.mark.integration
async def test_active_lease_prevents_second_claim() -> None:
    settings = get_settings()

    engine = create_async_engine(
        str(settings.database_url),
        pool_pre_ping=True,
    )
    session_factory = async_sessionmaker(
        bind=engine,
        expire_on_commit=False,
    )

    stale_at = datetime.now(UTC) - timedelta(minutes=10)

    payment = make_unknown_payment(
        updated_at=stale_at,
    )

    try:
        async with session_factory() as session:
            session.add(payment)
            await session.commit()

        now = datetime.now(UTC)

        async with session_factory() as session:
            repository = PaymentRepository(session)

            first_claim = await repository.claim_reconciliation_batch(
                stale_before=now,
                lease_expired_before=now,
                lease_token=uuid4(),
                lease_until=now + timedelta(minutes=1),
                batch_size=10,
            )

            await session.commit()

        assert first_claim == [
            payment.id,
        ]

        async with session_factory() as session:
            repository = PaymentRepository(session)

            second_claim = await repository.claim_reconciliation_batch(
                # Будущее время специально:
                # проверяем именно lease, но не updated_at.
                stale_before=now + timedelta(days=1),
                lease_expired_before=now,
                lease_token=uuid4(),
                lease_until=now + timedelta(minutes=1),
                batch_size=10,
            )

            await session.commit()

        assert second_claim == []
    finally:
        async with session_factory() as session:
            await session.execute(
                delete(Payment).where(
                    Payment.id == payment.id,
                )
            )
            await session.commit()

        await engine.dispose()


@pytest.mark.integration
async def test_expired_lease_can_be_reclaimed() -> None:
    settings = get_settings()

    engine = create_async_engine(
        str(settings.database_url),
        pool_pre_ping=True,
    )
    session_factory = async_sessionmaker(
        bind=engine,
        expire_on_commit=False,
    )

    stale_at = datetime.now(UTC) - timedelta(minutes=10)

    payment = make_unknown_payment(
        updated_at=stale_at,
    )

    try:
        async with session_factory() as session:
            session.add(payment)
            await session.commit()

        now = datetime.now(UTC)
        first_token = uuid4()

        async with session_factory() as session:
            repository = PaymentRepository(session)

            first_claim = await repository.claim_reconciliation_batch(
                stale_before=now,
                lease_expired_before=now,
                lease_token=first_token,
                lease_until=now + timedelta(minutes=1),
                batch_size=10,
            )

            await session.commit()

        assert first_claim == [
            payment.id,
        ]

        # Имитируем смерть первого worker:
        # lease истёк.
        async with session_factory() as session:
            persisted = await PaymentRepository(
                session,
            ).get_by_id(
                payment.id,
            )

            assert persisted is not None

            persisted.reconciliation_lease_until = datetime.now(UTC) - timedelta(seconds=1)

            await session.commit()

        second_token = uuid4()
        reclaim_now = datetime.now(UTC)

        async with session_factory() as session:
            repository = PaymentRepository(session)

            second_claim = await repository.claim_reconciliation_batch(
                stale_before=(reclaim_now + timedelta(days=1)),
                lease_expired_before=reclaim_now,
                lease_token=second_token,
                lease_until=(reclaim_now + timedelta(minutes=1)),
                batch_size=10,
            )

            await session.commit()

        assert second_claim == [
            payment.id,
        ]

        async with session_factory() as session:
            persisted = await PaymentRepository(
                session,
            ).get_by_id(
                payment.id,
            )

            assert persisted is not None
            assert persisted.reconciliation_lease_token == second_token
    finally:
        async with session_factory() as session:
            await session.execute(
                delete(Payment).where(
                    Payment.id == payment.id,
                )
            )
            await session.commit()

        await engine.dispose()


@pytest.mark.integration
async def test_manual_review_payment_is_not_claimed() -> None:
    settings = get_settings()

    engine = create_async_engine(
        str(settings.database_url),
        pool_pre_ping=True,
    )
    session_factory = async_sessionmaker(
        bind=engine,
        expire_on_commit=False,
    )

    stale_at = datetime.now(UTC) - timedelta(minutes=10)

    payment = make_unknown_payment(
        updated_at=stale_at,
        requires_manual_review=True,
    )

    try:
        async with session_factory() as session:
            session.add(payment)
            await session.commit()

        now = datetime.now(UTC)

        async with session_factory() as session:
            claimed = await PaymentRepository(
                session,
            ).claim_reconciliation_batch(
                stale_before=now + timedelta(days=1),
                lease_expired_before=now,
                lease_token=uuid4(),
                lease_until=now + timedelta(minutes=1),
                batch_size=10,
            )

            await session.commit()

        assert claimed == []
    finally:
        async with session_factory() as session:
            await session.execute(
                delete(Payment).where(
                    Payment.id == payment.id,
                )
            )
            await session.commit()

        await engine.dispose()


@pytest.mark.integration
async def test_stale_lease_token_cannot_access_claimed_payment() -> None:
    settings = get_settings()

    engine = create_async_engine(
        str(settings.database_url),
        pool_pre_ping=True,
    )
    session_factory = async_sessionmaker(
        bind=engine,
        expire_on_commit=False,
    )

    stale_at = datetime.now(UTC) - timedelta(minutes=10)

    payment = make_unknown_payment(
        updated_at=stale_at,
    )

    try:
        async with session_factory() as session:
            session.add(payment)
            await session.commit()

        now = datetime.now(UTC)
        valid_token = uuid4()

        async with session_factory() as session:
            await PaymentRepository(
                session,
            ).claim_reconciliation_batch(
                stale_before=now,
                lease_expired_before=now,
                lease_token=valid_token,
                lease_until=now + timedelta(minutes=1),
                batch_size=1,
            )

            await session.commit()

        async with session_factory() as session:
            repository = PaymentRepository(session)

            payment_for_stale_worker = await repository.get_claimed_payment_for_update(
                payment_id=payment.id,
                lease_token=uuid4(),
                now=datetime.now(UTC),
            )

            await session.rollback()

        assert payment_for_stale_worker is None

        async with session_factory() as session:
            repository = PaymentRepository(session)

            payment_for_owner = await repository.get_claimed_payment_for_update(
                payment_id=payment.id,
                lease_token=valid_token,
                now=datetime.now(UTC),
            )

            assert payment_for_owner is not None

            owner_payment_id = payment_for_owner.id

            await session.rollback()

        assert owner_payment_id == payment.id
    finally:
        async with session_factory() as session:
            await session.execute(
                delete(Payment).where(
                    Payment.id == payment.id,
                )
            )
            await session.commit()

        await engine.dispose()
