from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from uuid import UUID

from pydantic import (
    AnyHttpUrl,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
)

from app.models.payment import PaymentStatus


class Currency(StrEnum):
    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


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
    def validate_webhook_url(
        cls,
        value: AnyHttpUrl,
    ) -> AnyHttpUrl:
        if value.username is not None or value.password is not None:
            raise ValueError("webhook_url must not contain credentials")

        return value


class PaymentAccepted(BaseModel):
    payment_id: UUID
    status: PaymentStatus
    created_at: datetime


class PaymentDetail(BaseModel):
    model_config = ConfigDict(
        from_attributes=True,
    )

    payment_id: UUID = Field(validation_alias="id")
    amount: Decimal
    currency: Currency
    description: str | None
    metadata: dict[str, object] = Field(
        validation_alias="payment_metadata",
    )
    status: PaymentStatus
    idempotency_key: str
    webhook_url: AnyHttpUrl
    created_at: datetime
    processed_at: datetime | None
