from collections.abc import AsyncGenerator
from decimal import Decimal
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.dependencies import get_session
from app.api.payments import router as payments_router
from app.core.config import get_settings
from app.models.outbox_event import OutboxEvent
from app.models.payment import Payment, PaymentStatus
from app.repositories.outbox import OutboxRepository
from app.schemas.payment import PaymentCreate
from app.services.payment import PaymentService


@pytest.mark.integration
async def test_post_payment_creates_payment_and_outbox_and_is_idempotent(
    db_session: AsyncSession,
) -> None:
    settings = get_settings()

    application = FastAPI()
    application.state.settings = settings
    application.include_router(
        payments_router,
        prefix="/api/v1",
    )

    async def override_session() -> AsyncGenerator[AsyncSession]:
        yield db_session

    application.dependency_overrides[get_session] = override_session

    idempotency_key = f"integration-{uuid4()}"

    payload = {
        "amount": "1500.50",
        "currency": "RUB",
        "description": "Integration payment",
        "metadata": {
            "order_id": "integration-123",
        },
        "webhook_url": "https://example.com/webhook",
    }

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=application),
        base_url="http://testserver",
        headers={
            "X-API-Key": settings.api_key.get_secret_value(),
        },
    ) as client:
        first_response = await client.post(
            "/api/v1/payments",
            headers={
                "Idempotency-Key": idempotency_key,
            },
            json=payload,
        )

        second_response = await client.post(
            "/api/v1/payments",
            headers={
                "Idempotency-Key": idempotency_key,
            },
            json=payload,
        )

    assert first_response.status_code == 202
    assert second_response.status_code == 202

    first_body = first_response.json()
    second_body = second_response.json()

    assert first_body["payment_id"] == second_body["payment_id"]
    assert first_body["status"] == PaymentStatus.PENDING.value

    payment_result = await db_session.execute(
        select(Payment).where(
            Payment.idempotency_key == idempotency_key,
        )
    )
    payments = list(payment_result.scalars().all())

    assert len(payments) == 1

    payment = payments[0]

    assert payment.amount == Decimal("1500.50")
    assert payment.currency == "RUB"
    assert payment.status == PaymentStatus.PENDING
    assert payment.payment_metadata == {
        "order_id": "integration-123",
    }

    outbox_result = await db_session.execute(
        select(OutboxEvent).where(
            OutboxEvent.payment_id == payment.id,
        )
    )
    outbox_events = list(outbox_result.scalars().all())

    assert len(outbox_events) == 1

    outbox_event = outbox_events[0]

    assert outbox_event.event_type == "payment.process_requested"
    assert outbox_event.published_at is None
    assert outbox_event.payload["payment_id"] == str(payment.id)
    assert outbox_event.payload["attempt"] == 1


@pytest.mark.integration
async def test_payment_creation_rolls_back_when_outbox_creation_fails(
    db_session: AsyncSession,
) -> None:
    idempotency_key = f"rollback-{uuid4()}"

    data = PaymentCreate.model_validate(
        {
            "amount": "2500.00",
            "currency": "EUR",
            "description": "Rollback test",
            "metadata": {
                "order_id": "rollback-123",
            },
            "webhook_url": "https://example.com/webhook",
        }
    )

    service = PaymentService(db_session)

    with (
        patch.object(
            OutboxRepository,
            "add",
            new=AsyncMock(
                side_effect=RuntimeError("outbox insert failed"),
            ),
        ),
        pytest.raises(
            RuntimeError,
            match="outbox insert failed",
        ),
    ):
        await service.create_payment(
            data,
            idempotency_key=idempotency_key,
        )

    payment_result = await db_session.execute(
        select(Payment).where(
            Payment.idempotency_key == idempotency_key,
        )
    )

    assert payment_result.scalar_one_or_none() is None
