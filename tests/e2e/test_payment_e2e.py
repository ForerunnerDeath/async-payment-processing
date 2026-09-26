import asyncio
import os
from datetime import datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID, uuid4

import httpx
import pytest
from pydantic import BaseModel, TypeAdapter

API_BASE_URL = os.getenv(
    "E2E_API_BASE_URL",
    "http://127.0.0.1:8000",
).rstrip("/")

MOCK_PROVIDER_BASE_URL = os.getenv(
    "E2E_MOCK_PROVIDER_BASE_URL",
    "http://127.0.0.1:8001",
).rstrip("/")

WEBHOOK_TARGET_BASE_URL = os.getenv(
    "E2E_WEBHOOK_TARGET_BASE_URL",
    "http://mock-provider:8001/test/webhooks",
).rstrip("/")

API_KEY = os.getenv(
    "E2E_API_KEY",
    "local-dev-api-key",
)

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.skipif(
        os.getenv("RUN_E2E") != "1",
        reason="Set RUN_E2E=1 to run Docker E2E tests",
    ),
]


class PaymentAcceptedView(BaseModel):
    payment_id: UUID
    status: Literal[
        "pending",
        "succeeded",
        "failed",
    ]
    created_at: datetime


class PaymentDetailView(BaseModel):
    payment_id: UUID
    amount: Decimal
    currency: Literal["RUB", "USD", "EUR"]
    description: str | None
    metadata: dict[str, object]
    status: Literal[
        "pending",
        "succeeded",
        "failed",
    ]
    idempotency_key: str
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None


class CapturedWebhookPayloadView(BaseModel):
    event_id: UUID
    event_type: Literal[
        "payment.succeeded",
        "payment.failed",
    ]
    payment_id: UUID
    status: Literal[
        "succeeded",
        "failed",
    ]
    amount: Decimal
    currency: Literal["RUB", "USD", "EUR"]
    processed_at: datetime
    metadata: dict[str, object]


class CapturedWebhookView(BaseModel):
    payload: CapturedWebhookPayloadView
    webhook_id: UUID
    attempt: int


captured_webhooks_adapter = TypeAdapter(
    list[CapturedWebhookView],
)


def create_api_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        base_url=API_BASE_URL,
        headers={
            "X-API-Key": API_KEY,
        },
        timeout=5.0,
    )


def create_mock_provider_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        base_url=MOCK_PROVIDER_BASE_URL,
        timeout=5.0,
    )


async def configure_scenario(
    client: httpx.AsyncClient,
    scenario: str,
) -> None:
    response = await client.put(
        f"/test/scenario/{scenario}",
    )

    assert response.status_code == 200


async def clear_scenario(
    client: httpx.AsyncClient,
) -> None:
    response = await client.delete(
        "/test/scenario",
    )

    assert response.status_code == 204


async def clear_webhook_capture(
    client: httpx.AsyncClient,
    capture_id: str,
) -> None:
    response = await client.delete(
        f"/test/webhooks/{capture_id}",
    )

    assert response.status_code == 204


async def create_payment(
    client: httpx.AsyncClient,
    *,
    capture_id: str,
) -> PaymentAcceptedView:
    idempotency_key = f"e2e-{uuid4()}"

    response = await client.post(
        "/api/v1/payments",
        headers={
            "Idempotency-Key": idempotency_key,
        },
        json={
            "amount": "1500.50",
            "currency": "RUB",
            "description": "E2E payment",
            "metadata": {
                "test": "docker-e2e",
            },
            "webhook_url": (f"{WEBHOOK_TARGET_BASE_URL}/{capture_id}"),
        },
    )

    assert response.status_code == 202

    payment = PaymentAcceptedView.model_validate(
        response.json(),
    )

    assert payment.status == "pending"

    return payment


async def wait_for_payment_status(
    client: httpx.AsyncClient,
    *,
    payment_id: UUID,
    expected_status: Literal[
        "succeeded",
        "failed",
    ],
    timeout_seconds: float,
) -> PaymentDetailView:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_seconds

    last_status: str | None = None

    while True:
        response = await client.get(
            f"/api/v1/payments/{payment_id}",
        )

        assert response.status_code == 200

        payment = PaymentDetailView.model_validate(
            response.json(),
        )

        last_status = payment.status

        if payment.status == expected_status:
            return payment

        if loop.time() >= deadline:
            raise AssertionError(
                f"Payment did not reach {expected_status!r}; last status was {last_status!r}"
            )

        await asyncio.sleep(0.2)


async def wait_for_webhook(
    client: httpx.AsyncClient,
    *,
    capture_id: str,
    timeout_seconds: float,
) -> CapturedWebhookView:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_seconds

    while True:
        response = await client.get(
            f"/test/webhooks/{capture_id}",
        )

        assert response.status_code == 200

        captured = captured_webhooks_adapter.validate_python(
            response.json(),
        )

        if captured:
            return captured[0]

        if loop.time() >= deadline:
            raise AssertionError(f"Webhook {capture_id!r} was not delivered")

        await asyncio.sleep(0.2)


async def wait_for_provider_record(
    client: httpx.AsyncClient,
    *,
    payment_id: UUID,
    timeout_seconds: float,
) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_seconds

    while True:
        response = await client.get(
            f"/payments/by-idempotency-key/{payment_id}",
        )

        if response.status_code == 200:
            return

        assert response.status_code == 404

        if loop.time() >= deadline:
            raise AssertionError(f"Mock provider did not persist payment {payment_id}")

        await asyncio.sleep(0.2)


async def test_approved_payment_reaches_succeeded_and_delivers_webhook() -> None:
    capture_id = f"approved-{uuid4()}"

    async with (
        create_api_client() as api_client,
        create_mock_provider_client() as provider_client,
    ):
        await clear_webhook_capture(
            provider_client,
            capture_id,
        )
        await configure_scenario(
            provider_client,
            "approved",
        )

        try:
            accepted = await create_payment(
                api_client,
                capture_id=capture_id,
            )

            payment = await wait_for_payment_status(
                api_client,
                payment_id=accepted.payment_id,
                expected_status="succeeded",
                timeout_seconds=10.0,
            )

            webhook = await wait_for_webhook(
                provider_client,
                capture_id=capture_id,
                timeout_seconds=10.0,
            )

            assert payment.processed_at is not None
            assert webhook.attempt == 1

            assert webhook.payload.payment_id == accepted.payment_id
            assert webhook.payload.event_type == "payment.succeeded"
            assert webhook.payload.status == "succeeded"
            assert webhook.payload.amount == Decimal("1500.50")
            assert webhook.payload.currency == "RUB"
            assert webhook.payload.metadata == {
                "test": "docker-e2e",
            }

        finally:
            await clear_scenario(provider_client)


async def test_declined_payment_reaches_failed_and_delivers_webhook() -> None:
    capture_id = f"declined-{uuid4()}"

    async with (
        create_api_client() as api_client,
        create_mock_provider_client() as provider_client,
    ):
        await clear_webhook_capture(
            provider_client,
            capture_id,
        )
        await configure_scenario(
            provider_client,
            "declined",
        )

        try:
            accepted = await create_payment(
                api_client,
                capture_id=capture_id,
            )

            payment = await wait_for_payment_status(
                api_client,
                payment_id=accepted.payment_id,
                expected_status="failed",
                timeout_seconds=10.0,
            )

            webhook = await wait_for_webhook(
                provider_client,
                capture_id=capture_id,
                timeout_seconds=10.0,
            )

            assert payment.processed_at is not None
            assert webhook.attempt == 1

            assert webhook.payload.payment_id == accepted.payment_id
            assert webhook.payload.event_type == "payment.failed"
            assert webhook.payload.status == "failed"

        finally:
            await clear_scenario(provider_client)


async def test_timeout_after_processing_is_reconciled_and_delivers_webhook() -> None:
    capture_id = f"reconciliation-{uuid4()}"

    async with (
        create_api_client() as api_client,
        create_mock_provider_client() as provider_client,
    ):
        await clear_webhook_capture(
            provider_client,
            capture_id,
        )
        await configure_scenario(
            provider_client,
            "timeout-after-processing",
        )

        try:
            accepted = await create_payment(
                api_client,
                capture_id=capture_id,
            )

            await wait_for_provider_record(
                provider_client,
                payment_id=accepted.payment_id,
                timeout_seconds=10.0,
            )

            payment = await wait_for_payment_status(
                api_client,
                payment_id=accepted.payment_id,
                expected_status="succeeded",
                timeout_seconds=15.0,
            )

            webhook = await wait_for_webhook(
                provider_client,
                capture_id=capture_id,
                timeout_seconds=10.0,
            )

            assert payment.processed_at is not None
            assert webhook.attempt == 1

            assert webhook.payload.payment_id == accepted.payment_id
            assert webhook.payload.event_type == "payment.succeeded"
            assert webhook.payload.status == "succeeded"

        finally:
            await clear_scenario(provider_client)
