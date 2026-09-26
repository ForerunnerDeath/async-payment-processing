from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from uuid import UUID

from pydantic import (
    AnyHttpUrl,
    BaseModel,
    Field,
    field_validator,
)

from app.models.payment import PaymentStatus


class Currency(StrEnum):
    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class PublicPaymentStatus(StrEnum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


def to_public_payment_status(status: PaymentStatus | str) -> PublicPaymentStatus:
    internal_status = PaymentStatus(status)

    if internal_status is PaymentStatus.UNKNOWN:
        return PublicPaymentStatus.PENDING

    return PublicPaymentStatus(internal_status.value)


class PaymentCreate(BaseModel):
    amount: Decimal = Field(
        gt=Decimal("0"),
        max_digits=18,
        decimal_places=2,
    )
    currency: Currency
    description: str | None = None
    metadata: dict[str, object] = Field(default_factory=dict)
    webhook_url: AnyHttpUrl

    @field_validator("webhook_url")
    @classmethod
    def validate_webhook_url(cls, value: AnyHttpUrl) -> AnyHttpUrl:
        if value.username is not None or value.password is not None:
            raise ValueError("webhook_url must not contain credentials")

        return value


class PaymentAccepted(BaseModel):
    payment_id: UUID
    status: PublicPaymentStatus
    created_at: datetime


class PaymentDetail(BaseModel):
    payment_id: UUID
    amount: Decimal
    currency: Currency
    description: str | None
    metadata: dict[str, object]
    status: PublicPaymentStatus
    idempotency_key: str
    webhook_url: AnyHttpUrl
    created_at: datetime
    processed_at: datetime | None
