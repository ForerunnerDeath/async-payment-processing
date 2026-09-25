import json
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import httpx
import pytest

from app.clients.webhook import (
    WebhookClient,
    WebhookPermanentError,
    WebhookRetryableError,
)
from app.models.payment import PaymentStatus
from app.schemas.payment import Currency
from app.schemas.webhook import (
    WebhookEventType,
    WebhookPayload,
)


def make_payload() -> WebhookPayload:
    return WebhookPayload(
        event_id=uuid4(),
        event_type=WebhookEventType.PAYMENT_SUCCEEDED,
        payment_id=uuid4(),
        status=PaymentStatus.SUCCEEDED,
        amount=Decimal("500.15"),
        currency=Currency.RUB,
        processed_at=datetime(
            2026,
            9,
            25,
            10,
            0,
            tzinfo=UTC,
        ),
        metadata={
            "order_id": "order-123",
        },
    )


async def test_deliver_sends_expected_payload_and_headers() -> None:
    payload = make_payload()

    def handler(
        request: httpx.Request,
    ) -> httpx.Response:
        assert request.method == "POST"
        assert request.url == "https://client.test/webhook"

        assert request.headers["X-Webhook-Id"] == str(payload.event_id)
        assert request.headers["X-Webhook-Attempt"] == "1"
        assert request.headers["Content-Type"] == "application/json"

        body = json.loads(request.content)

        assert body["event_id"] == str(payload.event_id)
        assert body["payment_id"] == str(payload.payment_id)
        assert body["event_type"] == "payment.succeeded"
        assert body["status"] == "succeeded"
        assert body["amount"] == "500.15"
        assert body["currency"] == "RUB"
        assert body["metadata"] == {
            "order_id": "order-123",
        }

        return httpx.Response(
            status_code=204,
            request=request,
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
    ) as http_client:
        client = WebhookClient(
            http_client,
            timeout_seconds=5.0,
        )

        await client.deliver(
            url="https://client.test/webhook",
            payload=payload,
            event_id=payload.event_id,
            attempt=1,
        )


@pytest.mark.parametrize(
    "status_code",
    [
        408,
        429,
        500,
        502,
        503,
    ],
)
async def test_retryable_status_codes_raise_retryable_error(
    status_code: int,
) -> None:
    payload = make_payload()

    def handler(
        request: httpx.Request,
    ) -> httpx.Response:
        return httpx.Response(
            status_code=status_code,
            request=request,
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
    ) as http_client:
        client = WebhookClient(
            http_client,
            timeout_seconds=5.0,
        )

        with pytest.raises(
            WebhookRetryableError,
        ) as exc_info:
            await client.deliver(
                url="https://client.test/webhook",
                payload=payload,
                event_id=payload.event_id,
                attempt=2,
            )

    assert exc_info.value.status_code == status_code


async def test_network_error_is_retryable() -> None:
    payload = make_payload()

    def handler(
        request: httpx.Request,
    ) -> httpx.Response:
        raise httpx.ReadTimeout(
            "webhook timed out",
            request=request,
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
    ) as http_client:
        client = WebhookClient(
            http_client,
            timeout_seconds=5.0,
        )

        with pytest.raises(
            WebhookRetryableError,
            match="Webhook delivery failed",
        ):
            await client.deliver(
                url="https://client.test/webhook",
                payload=payload,
                event_id=payload.event_id,
                attempt=1,
            )


@pytest.mark.parametrize(
    "status_code",
    [
        301,
        302,
        400,
        401,
        403,
        404,
        422,
    ],
)
async def test_non_retryable_response_is_permanent(
    status_code: int,
) -> None:
    payload = make_payload()

    def handler(
        request: httpx.Request,
    ) -> httpx.Response:
        return httpx.Response(
            status_code=status_code,
            request=request,
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        follow_redirects=True,
    ) as http_client:
        client = WebhookClient(
            http_client,
            timeout_seconds=5.0,
        )

        with pytest.raises(
            WebhookPermanentError,
        ) as exc_info:
            await client.deliver(
                url="https://client.test/webhook",
                payload=payload,
                event_id=payload.event_id,
                attempt=1,
            )

    assert exc_info.value.status_code == status_code


async def test_retry_uses_same_webhook_id_and_new_attempt_number() -> None:
    payload = make_payload()

    received_headers: list[tuple[str, str]] = []

    def handler(
        request: httpx.Request,
    ) -> httpx.Response:
        received_headers.append(
            (
                request.headers["X-Webhook-Id"],
                request.headers["X-Webhook-Attempt"],
            )
        )

        return httpx.Response(
            status_code=204,
            request=request,
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
    ) as http_client:
        client = WebhookClient(
            http_client,
            timeout_seconds=5.0,
        )

        await client.deliver(
            url="https://client.test/webhook",
            payload=payload,
            event_id=payload.event_id,
            attempt=1,
        )
        await client.deliver(
            url="https://client.test/webhook",
            payload=payload,
            event_id=payload.event_id,
            attempt=2,
        )

    assert received_headers == [
        (
            str(payload.event_id),
            "1",
        ),
        (
            str(payload.event_id),
            "2",
        ),
    ]
