from decimal import Decimal
from uuid import uuid4

import httpx
import pytest

from app.clients.payment_provider import (
    IDEMPOTENCY_HEADER,
    PaymentProviderClient,
    ProviderAmbiguousOutcomeError,
    ProviderPermanentError,
    ProviderUnavailableError,
)
from app.schemas.payment import Currency
from app.schemas.provider import (
    ProviderPaymentRequest,
    ProviderPaymentStatus,
)


def make_request() -> ProviderPaymentRequest:
    return ProviderPaymentRequest(
        payment_id=uuid4(),
        amount=Decimal("500.15"),
        currency=Currency.RUB,
    )


def make_client(
    http_client: httpx.AsyncClient,
    *,
    max_attempts: int = 3,
    retry_base_delay_seconds: float = 0,
) -> PaymentProviderClient:
    return PaymentProviderClient(
        http_client,
        request_timeout_seconds=6.0,
        max_attempts=max_attempts,
        retry_base_delay_seconds=retry_base_delay_seconds,
        retry_max_delay_seconds=1.0,
        retry_total_timeout_seconds=10.0,
    )


async def test_process_payment_returns_approved_result() -> None:
    request_data = make_request()
    provider_payment_id = uuid4()

    def handler(
        request: httpx.Request,
    ) -> httpx.Response:
        assert request.headers[IDEMPOTENCY_HEADER] == str(request_data.payment_id)

        return httpx.Response(
            status_code=200,
            request=request,
            json={
                "provider_payment_id": str(
                    provider_payment_id,
                ),
                "status": "approved",
            },
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="http://provider.test",
    ) as http_client:
        result = await make_client(
            http_client,
        ).process_payment(request_data)

    assert result.provider_payment_id == provider_payment_id
    assert result.status is ProviderPaymentStatus.APPROVED


async def test_process_payment_returns_declined_result() -> None:
    request_data = make_request()

    def handler(
        request: httpx.Request,
    ) -> httpx.Response:
        return httpx.Response(
            status_code=200,
            request=request,
            json={
                "provider_payment_id": str(uuid4()),
                "status": "declined",
            },
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="http://provider.test",
    ) as http_client:
        result = await make_client(
            http_client,
        ).process_payment(request_data)

    assert result.status is ProviderPaymentStatus.DECLINED


async def test_retries_safe_503_and_keeps_same_idempotency_key() -> None:
    request_data = make_request()
    provider_payment_id = uuid4()

    attempts = 0
    keys: list[str] = []

    def handler(
        request: httpx.Request,
    ) -> httpx.Response:
        nonlocal attempts

        attempts += 1
        keys.append(
            request.headers[IDEMPOTENCY_HEADER],
        )

        if attempts < 3:
            return httpx.Response(
                status_code=503,
                request=request,
            )

        return httpx.Response(
            status_code=200,
            request=request,
            json={
                "provider_payment_id": str(
                    provider_payment_id,
                ),
                "status": "approved",
            },
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="http://provider.test",
    ) as http_client:
        result = await make_client(
            http_client,
        ).process_payment(request_data)

    assert attempts == 3
    assert keys == [
        str(request_data.payment_id),
        str(request_data.payment_id),
        str(request_data.payment_id),
    ]
    assert result.provider_payment_id == provider_payment_id


async def test_connect_error_retries_then_reports_unavailable() -> None:
    request_data = make_request()

    attempts = 0

    def handler(
        request: httpx.Request,
    ) -> httpx.Response:
        nonlocal attempts

        attempts += 1

        raise httpx.ConnectError(
            "provider unavailable",
            request=request,
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="http://provider.test",
    ) as http_client:
        with pytest.raises(
            ProviderUnavailableError,
            match="Payment provider is unavailable",
        ):
            await make_client(
                http_client,
            ).process_payment(request_data)

    assert attempts == 3


async def test_read_timeout_is_ambiguous_and_is_not_retried() -> None:
    request_data = make_request()

    attempts = 0

    def handler(
        request: httpx.Request,
    ) -> httpx.Response:
        nonlocal attempts

        attempts += 1

        raise httpx.ReadTimeout(
            "response timed out",
            request=request,
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="http://provider.test",
    ) as http_client:
        with pytest.raises(
            ProviderAmbiguousOutcomeError,
            match="outcome is unknown",
        ):
            await make_client(
                http_client,
            ).process_payment(request_data)

    assert attempts == 1


async def test_500_is_ambiguous_and_is_not_retried() -> None:
    request_data = make_request()

    attempts = 0

    def handler(
        request: httpx.Request,
    ) -> httpx.Response:
        nonlocal attempts

        attempts += 1

        return httpx.Response(
            status_code=500,
            request=request,
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="http://provider.test",
    ) as http_client:
        with pytest.raises(
            ProviderAmbiguousOutcomeError,
            match="outcome is unknown",
        ):
            await make_client(
                http_client,
            ).process_payment(request_data)

    assert attempts == 1


async def test_non_retryable_4xx_is_permanent() -> None:
    request_data = make_request()

    attempts = 0

    def handler(
        request: httpx.Request,
    ) -> httpx.Response:
        nonlocal attempts

        attempts += 1

        return httpx.Response(
            status_code=409,
            request=request,
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="http://provider.test",
    ) as http_client:
        with pytest.raises(
            ProviderPermanentError,
        ) as exc_info:
            await make_client(
                http_client,
            ).process_payment(request_data)

    assert attempts == 1
    assert exc_info.value.status_code == 409


async def test_invalid_success_response_is_ambiguous() -> None:
    request_data = make_request()

    attempts = 0

    def handler(
        request: httpx.Request,
    ) -> httpx.Response:
        nonlocal attempts

        attempts += 1

        return httpx.Response(
            status_code=200,
            request=request,
            json={
                "status": "approved",
            },
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="http://provider.test",
    ) as http_client:
        with pytest.raises(
            ProviderAmbiguousOutcomeError,
            match="invalid success response",
        ):
            await make_client(
                http_client,
            ).process_payment(request_data)

    assert attempts == 1


async def test_lookup_returns_stored_provider_result() -> None:
    payment_id = uuid4()
    provider_payment_id = uuid4()

    def handler(
        request: httpx.Request,
    ) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == (f"/payments/by-idempotency-key/{payment_id}")

        return httpx.Response(
            status_code=200,
            request=request,
            json={
                "provider_payment_id": str(
                    provider_payment_id,
                ),
                "status": "approved",
            },
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="http://provider.test",
    ) as http_client:
        result = await make_client(
            http_client,
        ).get_payment_by_idempotency_key(
            payment_id,
        )

    assert result is not None
    assert result.provider_payment_id == provider_payment_id


async def test_lookup_returns_none_for_unknown_payment() -> None:
    payment_id = uuid4()

    def handler(
        request: httpx.Request,
    ) -> httpx.Response:
        return httpx.Response(
            status_code=404,
            request=request,
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="http://provider.test",
    ) as http_client:
        result = await make_client(
            http_client,
        ).get_payment_by_idempotency_key(
            payment_id,
        )

    assert result is None
