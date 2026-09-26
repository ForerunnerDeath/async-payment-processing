import asyncio
import random
from dataclasses import dataclass
from typing import Annotated
from uuid import UUID, uuid4

from fastapi import FastAPI, Header, HTTPException, Response, status
from fastapi.responses import JSONResponse

from .schemas import (
    CapturedWebhook,
    CapturedWebhookPayload,
    MockScenario,
    ProcessPaymentRequest,
    ProcessPaymentResponse,
)

TIMEOUT_SCENARIO_DELAY_SECONDS = 10.0


@dataclass(frozen=True, slots=True)
class IdempotencyRecord:
    request: ProcessPaymentRequest
    response: ProcessPaymentResponse


@dataclass(slots=True)
class IdempotencyLockEntry:
    lock: asyncio.Lock
    users: int = 0


_idempotency_records: dict[UUID, IdempotencyRecord] = {}
_idempotency_locks: dict[UUID, IdempotencyLockEntry] = {}
_default_scenario: MockScenario | None = None
_captured_webhooks: dict[str, list[CapturedWebhook]] = {}


app = FastAPI(
    title="Async Payment Mock Provider",
    version="0.1.0",
)


@app.get("/health/live")
async def live() -> dict[str, str]:
    return {"status": "ok"}


@app.put("/test/scenario/{scenario}")
async def set_default_scenario(scenario: MockScenario) -> dict[str, str]:
    global _default_scenario

    _default_scenario = scenario

    return {
        "scenario": scenario.value,
    }


@app.delete("/test/scenario", status_code=status.HTTP_204_NO_CONTENT)
async def clear_default_scenario() -> Response:
    global _default_scenario

    _default_scenario = None

    return Response(
        status_code=status.HTTP_204_NO_CONTENT,
    )


@app.post("/test/webhooks/{capture_id}", status_code=status.HTTP_204_NO_CONTENT)
async def capture_webhook(
    capture_id: str,
    payload: CapturedWebhookPayload,
    webhook_id: Annotated[
        UUID,
        Header(alias="X-Webhook-Id"),
    ],
    attempt: Annotated[
        int,
        Header(
            alias="X-Webhook-Attempt",
            ge=1,
        ),
    ],
) -> Response:
    delivery = CapturedWebhook(
        payload=payload,
        webhook_id=webhook_id,
        attempt=attempt,
    )

    _captured_webhooks.setdefault(
        capture_id,
        [],
    ).append(delivery)

    return Response(
        status_code=status.HTTP_204_NO_CONTENT,
    )


@app.get("/test/webhooks/{capture_id}", response_model=list[CapturedWebhook])
async def get_captured_webhooks(capture_id: str) -> list[CapturedWebhook]:
    return list(
        _captured_webhooks.get(
            capture_id,
            [],
        )
    )


@app.delete("/test/webhooks/{capture_id}", status_code=status.HTTP_204_NO_CONTENT)
async def clear_captured_webhooks(capture_id: str) -> Response:
    _captured_webhooks.pop(
        capture_id,
        None,
    )

    return Response(
        status_code=status.HTTP_204_NO_CONTENT,
    )


@app.get("/payments/by-idempotency-key/{idempotency_key}", response_model=ProcessPaymentResponse)
async def get_payment_by_idempotency_key(idempotency_key: UUID) -> ProcessPaymentResponse:
    record = _idempotency_records.get(idempotency_key)

    if record is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Payment not found",
        )

    return record.response


@app.post("/process-payment", response_model=ProcessPaymentResponse)
async def process_payment(
    payment: ProcessPaymentRequest,
    idempotency_key: Annotated[
        UUID,
        Header(alias="Idempotency-Key"),
    ],
    mock_scenario: Annotated[
        MockScenario | None,
        Header(alias="X-Mock-Scenario"),
    ] = None,
) -> ProcessPaymentResponse | JSONResponse:
    if idempotency_key != payment.payment_id:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="Idempotency-Key must match payment_id",
        )

    effective_scenario = mock_scenario if mock_scenario is not None else _default_scenario

    if effective_scenario is MockScenario.ERROR_BEFORE_PROCESSING:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Mock provider unavailable before processing",
        )

    if effective_scenario is MockScenario.TIMEOUT_BEFORE_PROCESSING:
        await asyncio.sleep(TIMEOUT_SCENARIO_DELAY_SECONDS)

        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail="Mock provider timed out before processing",
        )

    if effective_scenario is None:
        await asyncio.sleep(
            random.uniform(2.0, 5.0),
        )

    lock_entry = _idempotency_locks.get(idempotency_key)

    if lock_entry is None:
        lock_entry = IdempotencyLockEntry(
            lock=asyncio.Lock(),
        )
        _idempotency_locks[idempotency_key] = lock_entry

    lock_entry.users += 1

    should_delay_response = False
    should_return_malformed_response = False

    try:
        async with lock_entry.lock:
            existing_record = _idempotency_records.get(
                idempotency_key,
            )

            if existing_record is not None:
                if existing_record.request != payment:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail=(
                            "Idempotency key was already used with a different payment payload"
                        ),
                    )

                return existing_record.response

            if effective_scenario is MockScenario.DECLINED:
                provider_status = "declined"
            elif effective_scenario in (
                MockScenario.APPROVED,
                MockScenario.TIMEOUT_AFTER_PROCESSING,
                MockScenario.MALFORMED_RESPONSE,
            ):
                provider_status = "approved"
            else:
                provider_status = "approved" if random.random() < 0.90 else "declined"

            response = ProcessPaymentResponse(
                provider_payment_id=uuid4(),
                status=provider_status,
            )

            _idempotency_records[idempotency_key] = IdempotencyRecord(
                request=payment,
                response=response,
            )

            should_delay_response = effective_scenario is MockScenario.TIMEOUT_AFTER_PROCESSING
            should_return_malformed_response = effective_scenario is MockScenario.MALFORMED_RESPONSE
    finally:
        lock_entry.users -= 1

        if lock_entry.users == 0 and _idempotency_locks.get(idempotency_key) is lock_entry:
            del _idempotency_locks[idempotency_key]

    if should_delay_response:
        await asyncio.sleep(
            TIMEOUT_SCENARIO_DELAY_SECONDS,
        )

    if should_return_malformed_response:
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "status": "approved",
            },
        )

    return response
