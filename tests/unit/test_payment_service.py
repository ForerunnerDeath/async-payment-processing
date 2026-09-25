from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.outbox_event import OutboxEvent
from app.models.payment import Payment
from app.repositories.outbox import OutboxRepository
from app.repositories.payment import PaymentRepository
from app.schemas.payment import PaymentCreate
from app.services.idempotency import build_request_fingerprint
from app.services.payment import (
    IdempotencyConflictError,
    PaymentService,
)

PAYMENT_ID = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")


def make_payment_data(
    **overrides: object,
) -> PaymentCreate:
    data: dict[str, object] = {
        "amount": "1500.50",
        "currency": "RUB",
        "description": "Order 123",
        "metadata": {
            "order_id": "123",
        },
        "webhook_url": "https://example.com/webhook",
    }
    data.update(overrides)

    return PaymentCreate.model_validate(data)


def make_existing_payment(
    data: PaymentCreate,
    *,
    idempotency_key: str = "order-123",
) -> Payment:
    return Payment(
        id=PAYMENT_ID,
        amount=data.amount,
        currency=data.currency.value,
        description=data.description,
        payment_metadata=dict(data.metadata),
        idempotency_key=idempotency_key,
        request_fingerprint=build_request_fingerprint(data),
        webhook_url=str(data.webhook_url),
    )


def make_session() -> MagicMock:
    session = MagicMock(spec=AsyncSession)
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    session.refresh = AsyncMock()

    return session


async def test_create_payment_creates_payment_and_outbox_in_one_transaction() -> None:
    data = make_payment_data()
    session = make_session()

    payment_repository = MagicMock(spec=PaymentRepository)
    payment_repository.get_by_idempotency_key = AsyncMock(
        return_value=None,
    )

    async def add_payment(payment: Payment) -> Payment:
        payment.id = PAYMENT_ID
        return payment

    payment_repository.add = AsyncMock(side_effect=add_payment)

    async def add_outbox_event(event: OutboxEvent) -> OutboxEvent:
        return event

    outbox_repository = MagicMock(spec=OutboxRepository)
    outbox_repository.add = AsyncMock(
        side_effect=add_outbox_event,
    )

    with (
        patch(
            "app.services.payment.PaymentRepository",
            return_value=payment_repository,
        ),
        patch(
            "app.services.payment.OutboxRepository",
            return_value=outbox_repository,
        ),
    ):
        service = PaymentService(session)

        result = await service.create_payment(
            data,
            idempotency_key="order-123",
        )

    assert result.created is True
    assert result.payment.id == PAYMENT_ID
    assert result.payment.request_fingerprint == build_request_fingerprint(data)

    payment_repository.add.assert_awaited_once()

    outbox_repository.add.assert_awaited_once()
    outbox_event = outbox_repository.add.await_args.args[0]

    assert isinstance(outbox_event, OutboxEvent)
    assert outbox_event.payment_id == PAYMENT_ID
    assert outbox_event.event_type == "payment.process_requested"

    assert outbox_event.payload["event_id"] == str(outbox_event.id)
    assert outbox_event.payload["event_type"] == "payment.process_requested"
    assert outbox_event.payload["schema_version"] == 1
    assert outbox_event.payload["payment_id"] == str(PAYMENT_ID)
    assert outbox_event.payload["attempt"] == 1

    session.commit.assert_awaited_once_with()
    session.rollback.assert_not_awaited()
    session.refresh.assert_awaited_once_with(result.payment)


async def test_create_payment_returns_existing_for_same_request() -> None:
    data = make_payment_data()
    existing_payment = make_existing_payment(data)
    session = make_session()

    payment_repository = MagicMock(spec=PaymentRepository)
    payment_repository.get_by_idempotency_key = AsyncMock(
        return_value=existing_payment,
    )
    payment_repository.add = AsyncMock()

    outbox_repository = MagicMock(spec=OutboxRepository)
    outbox_repository.add = AsyncMock()

    with (
        patch(
            "app.services.payment.PaymentRepository",
            return_value=payment_repository,
        ),
        patch(
            "app.services.payment.OutboxRepository",
            return_value=outbox_repository,
        ),
    ):
        service = PaymentService(session)

        result = await service.create_payment(
            data,
            idempotency_key="order-123",
        )

    assert result.payment is existing_payment
    assert result.created is False

    payment_repository.add.assert_not_awaited()
    outbox_repository.add.assert_not_awaited()
    session.commit.assert_not_awaited()


async def test_create_payment_rejects_existing_key_for_different_request() -> None:
    original = make_payment_data()
    changed = make_payment_data(
        description="Different order",
    )

    existing_payment = make_existing_payment(original)
    session = make_session()

    payment_repository = MagicMock(spec=PaymentRepository)
    payment_repository.get_by_idempotency_key = AsyncMock(
        return_value=existing_payment,
    )

    outbox_repository = MagicMock(spec=OutboxRepository)

    with (
        patch(
            "app.services.payment.PaymentRepository",
            return_value=payment_repository,
        ),
        patch(
            "app.services.payment.OutboxRepository",
            return_value=outbox_repository,
        ),
    ):
        service = PaymentService(session)

        with pytest.raises(IdempotencyConflictError):
            await service.create_payment(
                changed,
                idempotency_key="order-123",
            )

    session.commit.assert_not_awaited()


async def test_create_payment_recovers_from_concurrent_insert_race() -> None:
    data = make_payment_data()
    existing_payment = make_existing_payment(data)
    session = make_session()

    payment_repository = MagicMock(spec=PaymentRepository)
    payment_repository.get_by_idempotency_key = AsyncMock(
        side_effect=[
            None,
            existing_payment,
        ]
    )
    payment_repository.add = AsyncMock(
        side_effect=IntegrityError(
            "INSERT",
            {},
            Exception("unique violation"),
        ),
    )

    outbox_repository = MagicMock(spec=OutboxRepository)

    with (
        patch(
            "app.services.payment.PaymentRepository",
            return_value=payment_repository,
        ),
        patch(
            "app.services.payment.OutboxRepository",
            return_value=outbox_repository,
        ),
    ):
        service = PaymentService(session)

        result = await service.create_payment(
            data,
            idempotency_key="order-123",
        )

    assert result.payment is existing_payment
    assert result.created is False

    session.rollback.assert_awaited_once_with()
    session.commit.assert_not_awaited()


async def test_concurrent_insert_race_detects_different_request() -> None:
    data = make_payment_data()
    different_data = make_payment_data(
        description="Another request",
    )
    existing_payment = make_existing_payment(different_data)

    session = make_session()

    payment_repository = MagicMock(spec=PaymentRepository)
    payment_repository.get_by_idempotency_key = AsyncMock(
        side_effect=[
            None,
            existing_payment,
        ]
    )
    payment_repository.add = AsyncMock(
        side_effect=IntegrityError(
            "INSERT",
            {},
            Exception("unique violation"),
        ),
    )

    outbox_repository = MagicMock(spec=OutboxRepository)

    with (
        patch(
            "app.services.payment.PaymentRepository",
            return_value=payment_repository,
        ),
        patch(
            "app.services.payment.OutboxRepository",
            return_value=outbox_repository,
        ),
    ):
        service = PaymentService(session)

        with pytest.raises(IdempotencyConflictError):
            await service.create_payment(
                data,
                idempotency_key="order-123",
            )

    session.rollback.assert_awaited_once_with()


async def test_integrity_error_is_reraised_when_no_payment_exists() -> None:
    data = make_payment_data()
    session = make_session()

    integrity_error = IntegrityError(
        "INSERT",
        {},
        Exception("unexpected integrity error"),
    )

    payment_repository = MagicMock(spec=PaymentRepository)
    payment_repository.get_by_idempotency_key = AsyncMock(
        side_effect=[
            None,
            None,
        ]
    )
    payment_repository.add = AsyncMock(
        side_effect=integrity_error,
    )

    outbox_repository = MagicMock(spec=OutboxRepository)

    with (
        patch(
            "app.services.payment.PaymentRepository",
            return_value=payment_repository,
        ),
        patch(
            "app.services.payment.OutboxRepository",
            return_value=outbox_repository,
        ),
    ):
        service = PaymentService(session)

        with pytest.raises(IntegrityError) as exc_info:
            await service.create_payment(
                data,
                idempotency_key="order-123",
            )

    assert exc_info.value is integrity_error
    session.rollback.assert_awaited_once_with()


async def test_create_payment_rolls_back_on_unexpected_error() -> None:
    data = make_payment_data()
    session = make_session()

    payment_repository = MagicMock(spec=PaymentRepository)
    payment_repository.get_by_idempotency_key = AsyncMock(
        return_value=None,
    )
    payment_repository.add = AsyncMock(
        side_effect=RuntimeError("database failure"),
    )

    outbox_repository = MagicMock(spec=OutboxRepository)

    with (
        patch(
            "app.services.payment.PaymentRepository",
            return_value=payment_repository,
        ),
        patch(
            "app.services.payment.OutboxRepository",
            return_value=outbox_repository,
        ),
    ):
        service = PaymentService(session)

        with pytest.raises(RuntimeError, match="database failure"):
            await service.create_payment(
                data,
                idempotency_key="order-123",
            )

    session.rollback.assert_awaited_once_with()
    session.commit.assert_not_awaited()
