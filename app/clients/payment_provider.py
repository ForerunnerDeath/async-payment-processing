import asyncio
import random
import time
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from http import HTTPStatus
from uuid import UUID

import httpx
import structlog
from pydantic import ValidationError

from app.schemas.provider import ProviderPaymentRequest, ProviderPaymentResponse

logger = structlog.get_logger()

IDEMPOTENCY_HEADER = "Idempotency-Key"


class ProviderClientError(RuntimeError):
    pass


class ProviderPermanentError(ProviderClientError):
    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class ProviderUnavailableError(ProviderClientError):
    pass


class ProviderAmbiguousOutcomeError(ProviderClientError):
    pass


class PaymentProviderClient:
    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        request_timeout_seconds: float,
        max_attempts: int,
        retry_base_delay_seconds: float,
        retry_max_delay_seconds: float,
        retry_total_timeout_seconds: float,
    ) -> None:
        if request_timeout_seconds <= 0:
            raise ValueError(
                "request_timeout_seconds must be greater than zero",
            )

        if max_attempts < 1:
            raise ValueError(
                "max_attempts must be at least 1",
            )

        if retry_base_delay_seconds < 0:
            raise ValueError(
                "retry_base_delay_seconds must not be negative",
            )

        if retry_max_delay_seconds < retry_base_delay_seconds:
            raise ValueError(
                "retry_max_delay_seconds must be greater than or equal to retry_base_delay_seconds",
            )

        if retry_total_timeout_seconds <= 0:
            raise ValueError(
                "retry_total_timeout_seconds must be greater than zero",
            )

        self._client = client
        self._request_timeout_seconds = request_timeout_seconds
        self._max_attempts = max_attempts
        self._retry_base_delay_seconds = retry_base_delay_seconds
        self._retry_max_delay_seconds = retry_max_delay_seconds
        self._retry_total_timeout_seconds = retry_total_timeout_seconds

    async def process_payment(self, data: ProviderPaymentRequest) -> ProviderPaymentResponse:
        started_at = time.monotonic()

        for attempt in range(1, self._max_attempts + 1):
            try:
                response = await self._post_payment(data, started_at=started_at)
            except (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout) as exc:
                error = ProviderUnavailableError("Payment provider is unavailable")

                if attempt == self._max_attempts:
                    logger.error(
                        "payment_provider_retry_exhausted",
                        attempt=attempt,
                        max_attempts=self._max_attempts,
                        error_type=type(exc).__name__,
                        status_code=None,
                    )
                    raise error from exc

                await self._wait_before_retry(
                    attempt=attempt,
                    started_at=started_at,
                    error_type=type(exc).__name__,
                    status_code=None,
                )

                continue
            except (
                httpx.ReadTimeout,
                httpx.WriteTimeout,
                httpx.ReadError,
                httpx.WriteError,
                httpx.RemoteProtocolError,
            ) as exc:
                raise ProviderAmbiguousOutcomeError("Payment provider outcome is unknown") from exc
            except httpx.RequestError as exc:
                raise ProviderAmbiguousOutcomeError("Payment provider outcome is unknown") from exc

            status_code = response.status_code

            if 200 <= status_code < 300:
                return self._parse_success_response(response)

            if status_code in (
                HTTPStatus.TOO_MANY_REQUESTS,
                HTTPStatus.SERVICE_UNAVAILABLE,
            ):
                if attempt == self._max_attempts:
                    logger.error(
                        "payment_provider_retry_exhausted",
                        attempt=attempt,
                        max_attempts=self._max_attempts,
                        error_type="HTTPStatusError",
                        status_code=status_code,
                    )

                    raise ProviderUnavailableError("Payment provider is unavailable")

                retry_after_seconds = self._get_retry_after(response)

                await self._wait_before_retry(
                    attempt=attempt,
                    started_at=started_at,
                    error_type="HTTPStatusError",
                    status_code=status_code,
                    retry_after_seconds=retry_after_seconds,
                )

                continue

            if status_code == HTTPStatus.REQUEST_TIMEOUT or 500 <= status_code < 600:
                raise ProviderAmbiguousOutcomeError("Payment provider outcome is unknown")

            raise ProviderPermanentError(
                "Payment provider rejected the request", status_code=status_code
            )

        raise RuntimeError("Payment provider retry loop finished unexpectedly")

    async def get_payment_by_idempotency_key(
        self, idempotency_key: UUID
    ) -> ProviderPaymentResponse | None:
        try:
            response = await self._client.get(
                f"/payments/by-idempotency-key/{idempotency_key}",
                timeout=self._request_timeout_seconds,
            )
        except httpx.RequestError as exc:
            raise ProviderUnavailableError("Payment provider lookup failed") from exc

        if response.status_code == HTTPStatus.NOT_FOUND:
            return None

        if not 200 <= response.status_code < 300:
            if response.status_code >= 500:
                raise ProviderUnavailableError("Payment provider lookup failed")

            raise ProviderPermanentError(
                "Payment provider lookup was rejected",
                status_code=response.status_code,
            )

        try:
            return ProviderPaymentResponse.model_validate(response.json())
        except (ValueError, ValidationError) as exc:
            raise ProviderUnavailableError(
                "Payment provider returned invalid lookup response"
            ) from exc

    async def _post_payment(
        self, data: ProviderPaymentRequest, *, started_at: float
    ) -> httpx.Response:
        remaining_time = self._retry_total_timeout_seconds - (time.monotonic() - started_at)

        if remaining_time <= 0:
            raise ProviderUnavailableError("Payment provider retry time budget exhausted")

        timeout_seconds = min(self._request_timeout_seconds, remaining_time)

        try:
            async with asyncio.timeout(remaining_time):
                return await self._client.post(
                    "/process-payment",
                    json=data.model_dump(mode="json"),
                    headers={IDEMPOTENCY_HEADER: str(data.payment_id)},
                    timeout=timeout_seconds,
                )
        except TimeoutError as exc:
            raise ProviderAmbiguousOutcomeError(
                "Payment provider retry time budget exhausted during request",
            ) from exc

    def _parse_success_response(self, response: httpx.Response) -> ProviderPaymentResponse:
        try:
            return ProviderPaymentResponse.model_validate(response.json())
        except (ValueError, ValidationError) as exc:
            raise ProviderAmbiguousOutcomeError(
                "Payment provider returned an invalid success response",
            ) from exc

    async def _wait_before_retry(
        self,
        *,
        attempt: int,
        started_at: float,
        error_type: str,
        status_code: int | None,
        retry_after_seconds: float | None = None,
    ) -> None:
        if retry_after_seconds is None:
            retry_interval = min(
                self._retry_base_delay_seconds * 2 ** (attempt - 1),
                self._retry_max_delay_seconds,
            )

            delay = random.uniform(0, retry_interval)
        else:
            delay = retry_after_seconds

        remaining_time = self._retry_total_timeout_seconds - (time.monotonic() - started_at)

        if delay >= remaining_time:
            raise ProviderUnavailableError("Payment provider retry time budget exhausted")

        logger.warning(
            "payment_provider_retry_scheduled",
            attempt=attempt,
            next_attempt=attempt + 1,
            max_attempts=self._max_attempts,
            delay_seconds=round(delay, 3),
            error_type=error_type,
            status_code=status_code,
        )

        await asyncio.sleep(delay)

    def _get_retry_after(self, response: httpx.Response) -> float | None:
        retry_after = response.headers.get("Retry-After")

        if retry_after is None:
            return None

        return self._parse_retry_after(retry_after)

    @staticmethod
    def _parse_retry_after(retry_after: str) -> float | None:
        try:
            parsed_retry_after = float(retry_after)
        except ValueError:
            try:
                retry_at = parsedate_to_datetime(retry_after)
            except TypeError, ValueError, IndexError:
                return None

            if retry_at.tzinfo is None:
                retry_at = retry_at.replace(tzinfo=UTC)

            delay = (retry_at - datetime.now(UTC)).total_seconds()

            return max(delay, 0.0)

        if parsed_retry_after < 0:
            return None

        return parsed_retry_after
