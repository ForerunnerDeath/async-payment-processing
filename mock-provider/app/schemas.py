from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field


class MockScenario(StrEnum):
    APPROVED = "approved"
    DECLINED = "declined"
    ERROR_BEFORE_PROCESSING = "error-before-processing"
    TIMEOUT_BEFORE_PROCESSING = "timeout-before-processing"
    TIMEOUT_AFTER_PROCESSING = "timeout-after-processing"
    MALFORMED_RESPONSE = "malformed-response"


class ProcessPaymentRequest(BaseModel):
    payment_id: UUID
    amount: Decimal = Field(
        gt=Decimal("0"),
        max_digits=18,
        decimal_places=2,
    )
    currency: Literal["RUB", "USD", "EUR"]


class ProcessPaymentResponse(BaseModel):
    provider_payment_id: UUID
    status: Literal["approved", "declined"]


class CapturedWebhookPayload(BaseModel):
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
    amount: Decimal = Field(
        gt=Decimal("0"),
        max_digits=18,
        decimal_places=2,
    )
    currency: Literal["RUB", "USD", "EUR"]
    processed_at: datetime
    metadata: dict[str, object]


class CapturedWebhook(BaseModel):
    payload: CapturedWebhookPayload
    webhook_id: UUID
    attempt: int = Field(ge=1)
