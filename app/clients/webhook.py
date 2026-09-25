from uuid import UUID

import httpx

from app.schemas.webhook import WebhookPayload


class WebhookClientError(RuntimeError):
    pass


class WebhookRetryableError(WebhookClientError):
    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class WebhookPermanentError(WebhookClientError):
    def __init__(self, message: str, *, status_code: int) -> None:
        super().__init__(message)
        self.status_code = status_code


class WebhookClient:
    def __init__(self, client: httpx.AsyncClient, *, timeout_seconds: float) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be greater than zero")

        self._client = client
        self._timeout_seconds = timeout_seconds

    async def deliver(
        self, *, url: str, payload: WebhookPayload, event_id: UUID, attempt: int
    ) -> None:
        if attempt < 1:
            raise ValueError("attempt must be at least 1")

        try:
            response = await self._client.post(
                url,
                json=payload.model_dump(mode="json"),
                headers={
                    "Content-Type": "application/json",
                    "X-Webhook-Id": str(event_id),
                    "X-Webhook-Attempt": str(attempt),
                },
                timeout=self._timeout_seconds,
                follow_redirects=False,
            )
        except httpx.RequestError as exc:
            raise WebhookRetryableError("Webhook delivery failed") from exc

        status_code = response.status_code

        if 200 <= status_code < 300:
            return

        if status_code in (408, 429) or 500 <= status_code < 600:
            raise WebhookRetryableError(
                "Webhook endpoint returned a retryable response",
                status_code=status_code,
            )

        raise WebhookPermanentError(
            "Webhook endpoint returned a permanent failure",
            status_code=status_code,
        )
