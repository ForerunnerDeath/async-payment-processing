from types import ModuleType
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import httpx
import pytest


def make_payload(
    payment_id: str,
    *,
    amount: str = "500.15",
) -> dict[str, str]:
    return {
        "payment_id": payment_id,
        "amount": amount,
        "currency": "RUB",
    }


def create_client(
    provider_module: ModuleType,
) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(
            app=provider_module.app,
        ),
        base_url="http://provider.test",
    )


async def test_approved_scenario_is_idempotent(
    provider_module: ModuleType,
) -> None:
    payment_id = uuid4()

    async with create_client(provider_module) as client:
        headers = {
            "Idempotency-Key": str(payment_id),
            "X-Mock-Scenario": "approved",
        }
        payload = make_payload(str(payment_id))

        first = await client.post(
            "/process-payment",
            json=payload,
            headers=headers,
        )
        second = await client.post(
            "/process-payment",
            json=payload,
            headers=headers,
        )

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json() == second.json()
    assert first.json()["status"] == "approved"


async def test_declined_scenario_returns_business_decline(
    provider_module: ModuleType,
) -> None:
    payment_id = uuid4()

    async with create_client(provider_module) as client:
        response = await client.post(
            "/process-payment",
            json=make_payload(str(payment_id)),
            headers={
                "Idempotency-Key": str(payment_id),
                "X-Mock-Scenario": "declined",
            },
        )

    assert response.status_code == 200
    assert response.json()["status"] == "declined"


async def test_reused_key_with_different_payload_returns_conflict(
    provider_module: ModuleType,
) -> None:
    payment_id = uuid4()

    async with create_client(provider_module) as client:
        headers = {
            "Idempotency-Key": str(payment_id),
            "X-Mock-Scenario": "approved",
        }

        first = await client.post(
            "/process-payment",
            json=make_payload(str(payment_id)),
            headers=headers,
        )

        conflicting = await client.post(
            "/process-payment",
            json=make_payload(
                str(payment_id),
                amount="700.00",
            ),
            headers=headers,
        )

    assert first.status_code == 200
    assert conflicting.status_code == 409


async def test_error_before_processing_does_not_store_payment(
    provider_module: ModuleType,
) -> None:
    payment_id = uuid4()

    async with create_client(provider_module) as client:
        response = await client.post(
            "/process-payment",
            json=make_payload(str(payment_id)),
            headers={
                "Idempotency-Key": str(payment_id),
                "X-Mock-Scenario": "error-before-processing",
            },
        )

        lookup = await client.get(
            f"/payments/by-idempotency-key/{payment_id}",
        )

    assert response.status_code == 503
    assert lookup.status_code == 404


async def test_timeout_after_processing_stores_result_before_delay(
    provider_module: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payment_id = uuid4()

    sleep_started = False

    async def delayed_response(_: float) -> None:
        nonlocal sleep_started
        sleep_started = True

    monkeypatch.setattr(
        provider_module.asyncio,
        "sleep",
        delayed_response,
    )

    async with create_client(provider_module) as client:
        response = await client.post(
            "/process-payment",
            json=make_payload(str(payment_id)),
            headers={
                "Idempotency-Key": str(payment_id),
                "X-Mock-Scenario": "timeout-after-processing",
            },
        )

        lookup = await client.get(
            f"/payments/by-idempotency-key/{payment_id}",
        )

    assert sleep_started is True
    assert response.status_code == 200
    assert lookup.status_code == 200
    assert lookup.json() == response.json()


async def test_malformed_response_still_stores_provider_result(
    provider_module: ModuleType,
) -> None:
    payment_id = uuid4()

    async with create_client(provider_module) as client:
        response = await client.post(
            "/process-payment",
            json=make_payload(str(payment_id)),
            headers={
                "Idempotency-Key": str(payment_id),
                "X-Mock-Scenario": "malformed-response",
            },
        )

        lookup = await client.get(
            f"/payments/by-idempotency-key/{payment_id}",
        )

    assert response.status_code == 200
    assert response.json() == {
        "status": "approved",
    }

    assert lookup.status_code == 200
    assert "provider_payment_id" in lookup.json()
    assert lookup.json()["status"] == "approved"


async def test_normal_processing_waits_two_to_five_seconds_and_uses_90_10_split(
    provider_module: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payment_id = uuid4()

    sleep_mock = AsyncMock()

    monkeypatch.setattr(
        provider_module.asyncio,
        "sleep",
        sleep_mock,
    )
    monkeypatch.setattr(
        provider_module.random,
        "uniform",
        Mock(return_value=3.5),
    )
    monkeypatch.setattr(
        provider_module.random,
        "random",
        Mock(return_value=0.89),
    )

    async with create_client(provider_module) as client:
        response = await client.post(
            "/process-payment",
            json=make_payload(str(payment_id)),
            headers={
                "Idempotency-Key": str(payment_id),
            },
        )

    assert response.status_code == 200
    assert response.json()["status"] == "approved"

    sleep_mock.assert_awaited_once_with(3.5)


async def test_idempotency_key_must_match_payment_id(
    provider_module: ModuleType,
) -> None:
    payment_id = uuid4()

    async with create_client(provider_module) as client:
        response = await client.post(
            "/process-payment",
            json=make_payload(str(payment_id)),
            headers={
                "Idempotency-Key": str(uuid4()),
                "X-Mock-Scenario": "approved",
            },
        )

    assert response.status_code == 422
