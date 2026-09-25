from datetime import datetime
from enum import StrEnum
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field


class EventType(StrEnum):
    PAYMENT_PROCESS_REQUESTED = "payment.process_requested"
    WEBHOOK_DELIVERY_REQUESTED = "webhook.delivery_requested"


class EventEnvelope(BaseModel):
    event_id: UUID
    event_type: EventType
    schema_version: Literal[1] = 1
    payment_id: UUID
    attempt: int = Field(default=1, ge=1)
    occurred_at: datetime
