from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Self
from uuid import UUID

from pydantic import BaseModel, Field, model_validator

from app.models.payment import PaymentStatus
from app.schemas.payment import Currency


class WebhookEventType(StrEnum):
    PAYMENT_SUCCEEDED = "payment.succeeded"
    PAYMENT_FAILED = "payment.failed"


class WebhookPayload(BaseModel):
    event_id: UUID
    event_type: WebhookEventType
    payment_id: UUID
    status: PaymentStatus
    amount: Decimal = Field(gt=Decimal("0"), max_digits=18, decimal_places=2)
    currency: Currency
    processed_at: datetime
    metadata: dict[str, object]

    @model_validator(mode="after")
    def validate_terminal_status(self) -> Self:
        expected_event_type = {
            PaymentStatus.SUCCEEDED: WebhookEventType.PAYMENT_SUCCEEDED,
            PaymentStatus.FAILED: WebhookEventType.PAYMENT_FAILED,
        }.get(self.status)

        if expected_event_type is None:
            raise ValueError("Webhook payload requires a terminal payment status")

        if self.event_type != expected_event_type:
            raise ValueError("Webhook event_type does not match payment status")

        return self
