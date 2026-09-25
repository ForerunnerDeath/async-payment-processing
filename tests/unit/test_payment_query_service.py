from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncSession

from app.models.payment import Payment
from app.repositories.payment import PaymentRepository
from app.services.payment_query import PaymentQueryService


async def test_get_payment_returns_repository_result() -> None:
    payment_id = uuid4()
    payment = MagicMock(spec=Payment)

    session = MagicMock(spec=AsyncSession)

    repository = MagicMock(spec=PaymentRepository)
    repository.get_by_id = AsyncMock(
        return_value=payment,
    )

    with patch(
        "app.services.payment_query.PaymentRepository",
        return_value=repository,
    ):
        service = PaymentQueryService(session)

        result = await service.get_payment(payment_id)

    assert result is payment
    repository.get_by_id.assert_awaited_once_with(payment_id)
