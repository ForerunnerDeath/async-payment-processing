from datetime import datetime
from uuid import UUID

from sqlalchemy import or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.payment import Payment, PaymentStatus


class PaymentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_by_idempotency_key(self, idempotency_key: str) -> Payment | None:
        statement = select(Payment).where(
            Payment.idempotency_key == idempotency_key,
        )

        result = await self._session.execute(statement)

        return result.scalar_one_or_none()

    async def get_by_id(self, payment_id: UUID) -> Payment | None:
        statement = select(Payment).where(
            Payment.id == payment_id,
        )

        result = await self._session.execute(statement)

        return result.scalar_one_or_none()

    async def add(self, payment: Payment) -> Payment:
        self._session.add(payment)
        await self._session.flush()

        return payment

    async def get_by_id_for_update(self, payment_id: UUID) -> Payment | None:
        statement = (
            select(Payment)
            .where(
                Payment.id == payment_id,
            )
            .with_for_update()
        )

        result = await self._session.execute(statement)

        return result.scalar_one_or_none()

    async def claim_reconciliation_batch(
        self,
        *,
        stale_before: datetime,
        lease_expired_before: datetime,
        lease_token: UUID,
        lease_until: datetime,
        batch_size: int,
    ) -> list[UUID]:
        stmt = (
            select(Payment)
            .where(
                Payment.status == PaymentStatus.UNKNOWN,
                Payment.requires_manual_review.is_(False),
                Payment.updated_at <= stale_before,
                or_(
                    Payment.reconciliation_lease_until.is_(None),
                    Payment.reconciliation_lease_until <= lease_expired_before,
                ),
            )
            .order_by(
                Payment.updated_at,
                Payment.id,
            )
            .limit(batch_size)
            .with_for_update(
                skip_locked=True,
            )
        )

        result = await self._session.execute(stmt)

        payments = list(
            result.scalars().all(),
        )

        for payment in payments:
            payment.reconciliation_lease_token = lease_token
            payment.reconciliation_lease_until = lease_until

        await self._session.flush()

        return [payment.id for payment in payments]

    async def renew_reconciliation_lease(
        self,
        *,
        payment_id: UUID,
        lease_token: UUID,
        now: datetime,
        lease_until: datetime,
    ) -> bool:
        stmt = (
            update(Payment)
            .where(
                Payment.id == payment_id,
                Payment.status == PaymentStatus.UNKNOWN,
                Payment.requires_manual_review.is_(False),
                Payment.reconciliation_lease_token == lease_token,
                Payment.reconciliation_lease_until.is_not(None),
                Payment.reconciliation_lease_until > now,
            )
            .values(
                reconciliation_lease_until=lease_until,
            )
            .returning(Payment.id)
        )

        result = await self._session.execute(stmt)

        return result.scalar_one_or_none() is not None

    async def get_claimed_payment_for_update(
        self,
        *,
        payment_id: UUID,
        lease_token: UUID,
        now: datetime,
    ) -> Payment | None:
        stmt = (
            select(Payment)
            .where(
                Payment.id == payment_id,
                Payment.status == PaymentStatus.UNKNOWN,
                Payment.requires_manual_review.is_(False),
                Payment.reconciliation_lease_token == lease_token,
                Payment.reconciliation_lease_until.is_not(None),
                Payment.reconciliation_lease_until > now,
            )
            .with_for_update()
        )

        result = await self._session.execute(stmt)

        return result.scalar_one_or_none()
