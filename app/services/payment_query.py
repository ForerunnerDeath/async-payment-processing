from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from app.models.payment import Payment
from app.repositories.payment import PaymentRepository


class PaymentQueryService:
    def __init__(self, session: AsyncSession) -> None:
        self._repository = PaymentRepository(session)

    async def get_payment(self, payment_id: UUID) -> Payment | None:
        return await self._repository.get_by_id(payment_id)
