from decimal import Decimal
from enum import StrEnum
from uuid import UUID

from pydantic import BaseModel, Field

from app.schemas.payment import Currency


class ProviderPaymentStatus(StrEnum):
    APPROVED = "approved"
    DECLINED = "declined"


class ProviderPaymentRequest(BaseModel):
    payment_id: UUID
    amount: Decimal = Field(gt=Decimal("0"), max_digits=18, decimal_places=2)
    currency: Currency


class ProviderPaymentResponse(BaseModel):
    provider_payment_id: UUID
    status: ProviderPaymentStatus
