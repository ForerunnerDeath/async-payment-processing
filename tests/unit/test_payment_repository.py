from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncSession

from app.models.payment import Payment
from app.repositories.payment import PaymentRepository


async def test_get_by_idempotency_key_returns_payment() -> None:
    payment = MagicMock(spec=Payment)

    result = MagicMock()
    result.scalar_one_or_none.return_value = payment

    session = MagicMock(spec=AsyncSession)
    session.execute = AsyncMock(return_value=result)

    repository = PaymentRepository(session)

    returned_payment = await repository.get_by_idempotency_key(
        "order-123",
    )

    assert returned_payment is payment
    session.execute.assert_awaited_once()


async def test_get_by_id_returns_payment() -> None:
    payment_id = uuid4()
    payment = MagicMock(spec=Payment)

    result = MagicMock()
    result.scalar_one_or_none.return_value = payment

    session = MagicMock(spec=AsyncSession)
    session.execute = AsyncMock(return_value=result)

    repository = PaymentRepository(session)

    returned_payment = await repository.get_by_id(payment_id)

    assert returned_payment is payment
    session.execute.assert_awaited_once()


async def test_add_payment_flushes_session() -> None:
    payment = MagicMock(spec=Payment)

    session = MagicMock(spec=AsyncSession)
    session.flush = AsyncMock()

    repository = PaymentRepository(session)

    returned_payment = await repository.add(payment)

    assert returned_payment is payment
    session.add.assert_called_once_with(payment)
    session.flush.assert_awaited_once_with()
