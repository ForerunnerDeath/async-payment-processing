from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import httpx
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.dependencies import get_session
from app.api.payments import router
from app.models.payment import Payment, PaymentStatus
from app.services.payment import (
    IdempotencyConflictError,
    PaymentCreationResult,
    PaymentService,
)
from app.services.payment_query import PaymentQueryService

PAYMENT_ID = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
CREATED_AT = datetime(2026, 9, 25, 8, 0, tzinfo=UTC)


async def override_session() -> AsyncSession:
    return MagicMock(spec=AsyncSession)


def create_test_app() -> FastAPI:
    application = FastAPI()
    application.include_router(router, prefix="/api/v1")
    application.dependency_overrides[get_session] = override_session

    settings = MagicMock()
    settings.api_key.get_secret_value.return_value = "test-api-key"
    application.state.settings = settings

    return application


def create_client(application: FastAPI) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=application),
        base_url="http://testserver",
        headers={"X-API-Key": "test-api-key"},
    )


def make_payment(
    *,
    status: PaymentStatus = PaymentStatus.PENDING,
) -> Payment:
    return Payment(
        id=PAYMENT_ID,
        amount=Decimal("1500.50"),
        currency="RUB",
        description="Order 123",
        payment_metadata={"order_id": "123"},
        status=status,
        idempotency_key="order-123",
        request_fingerprint="a" * 64,
        webhook_url="https://example.com/webhook",
        created_at=CREATED_AT,
        updated_at=CREATED_AT,
    )


async def test_create_payment_returns_202() -> None:
    application = create_test_app()
    payment = make_payment()

    service = MagicMock(spec=PaymentService)
    service.create_payment = AsyncMock(
        return_value=PaymentCreationResult(
            payment=payment,
            created=True,
        )
    )

    with patch(
        "app.api.payments.PaymentService",
        return_value=service,
    ):
        async with create_client(application) as client:
            response = await client.post(
                "/api/v1/payments",
                headers={
                    "Idempotency-Key": "order-123",
                },
                json={
                    "amount": "1500.50",
                    "currency": "RUB",
                    "description": "Order 123",
                    "metadata": {
                        "order_id": "123",
                    },
                    "webhook_url": "https://example.com/webhook",
                },
            )

    assert response.status_code == 202
    assert response.json() == {
        "payment_id": str(PAYMENT_ID),
        "status": "pending",
        "created_at": CREATED_AT.isoformat().replace("+00:00", "Z"),
    }

    service.create_payment.assert_awaited_once()


async def test_create_payment_requires_idempotency_key() -> None:
    application = create_test_app()

    async with create_client(application) as client:
        response = await client.post(
            "/api/v1/payments",
            json={
                "amount": "1500.50",
                "currency": "RUB",
                "webhook_url": "https://example.com/webhook",
            },
        )

    assert response.status_code == 422


async def test_create_payment_returns_409_for_idempotency_conflict() -> None:
    application = create_test_app()

    service = MagicMock(spec=PaymentService)
    service.create_payment = AsyncMock(
        side_effect=IdempotencyConflictError(
            "Idempotency-Key was already used with a different request",
        )
    )

    with patch(
        "app.api.payments.PaymentService",
        return_value=service,
    ):
        async with create_client(application) as client:
            response = await client.post(
                "/api/v1/payments",
                headers={
                    "Idempotency-Key": "order-123",
                },
                json={
                    "amount": "1500.50",
                    "currency": "RUB",
                    "webhook_url": "https://example.com/webhook",
                },
            )

    assert response.status_code == 409


async def test_get_payment_returns_payment() -> None:
    application = create_test_app()
    payment = make_payment()

    service = MagicMock(spec=PaymentQueryService)
    service.get_payment = AsyncMock(return_value=payment)

    with patch(
        "app.api.payments.PaymentQueryService",
        return_value=service,
    ):
        async with create_client(application) as client:
            response = await client.get(
                f"/api/v1/payments/{PAYMENT_ID}",
            )

    assert response.status_code == 200

    body = response.json()

    assert body["payment_id"] == str(PAYMENT_ID)
    assert body["amount"] == "1500.50"
    assert body["currency"] == "RUB"
    assert body["status"] == "pending"
    assert body["metadata"] == {"order_id": "123"}
    assert body["idempotency_key"] == "order-123"


async def test_get_payment_returns_404() -> None:
    application = create_test_app()

    service = MagicMock(spec=PaymentQueryService)
    service.get_payment = AsyncMock(return_value=None)

    with patch(
        "app.api.payments.PaymentQueryService",
        return_value=service,
    ):
        async with create_client(application) as client:
            response = await client.get(
                f"/api/v1/payments/{PAYMENT_ID}",
            )

    assert response.status_code == 404
    assert response.json() == {
        "detail": "Payment not found",
    }


async def test_get_payment_maps_internal_unknown_to_pending() -> None:
    application = create_test_app()
    payment = make_payment(
        status=PaymentStatus.UNKNOWN,
    )

    service = MagicMock(spec=PaymentQueryService)
    service.get_payment = AsyncMock(return_value=payment)

    with patch(
        "app.api.payments.PaymentQueryService",
        return_value=service,
    ):
        async with create_client(application) as client:
            response = await client.get(
                f"/api/v1/payments/{PAYMENT_ID}",
            )

    assert response.status_code == 200
    assert response.json()["status"] == "pending"


async def test_create_payment_maps_internal_unknown_to_pending() -> None:
    application = create_test_app()
    payment = make_payment(
        status=PaymentStatus.UNKNOWN,
    )

    service = MagicMock(spec=PaymentService)
    service.create_payment = AsyncMock(
        return_value=PaymentCreationResult(
            payment=payment,
            created=False,
        )
    )

    with patch(
        "app.api.payments.PaymentService",
        return_value=service,
    ):
        async with create_client(application) as client:
            response = await client.post(
                "/api/v1/payments",
                headers={
                    "Idempotency-Key": "order-123",
                },
                json={
                    "amount": "1500.50",
                    "currency": "RUB",
                    "description": "Order 123",
                    "metadata": {
                        "order_id": "123",
                    },
                    "webhook_url": "https://example.com/webhook",
                },
            )

    assert response.status_code == 202
    assert response.json()["status"] == "pending"


async def test_openapi_does_not_expose_internal_unknown_status() -> None:
    application = create_test_app()

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=application),
        base_url="http://testserver",
    ) as client:
        response = await client.get("/openapi.json")

    assert response.status_code == 200

    schema = response.json()

    public_status_schema = schema["components"]["schemas"]["PublicPaymentStatus"]

    assert public_status_schema["enum"] == [
        "pending",
        "succeeded",
        "failed",
    ]
    assert "unknown" not in public_status_schema["enum"]
