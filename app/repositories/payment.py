from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.payment import Payment


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
