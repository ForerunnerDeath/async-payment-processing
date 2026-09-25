import hashlib
import json
from decimal import Decimal

from app.schemas.payment import PaymentCreate

_AMOUNT_QUANT = Decimal("0.01")


def build_request_fingerprint(data: PaymentCreate) -> str:
    canonical_payload = {
        "amount": format(
            data.amount.quantize(_AMOUNT_QUANT),
            "f",
        ),
        "currency": data.currency.value,
        "description": data.description,
        "metadata": data.metadata,
        "webhook_url": str(data.webhook_url),
    }

    serialized = json.dumps(
        canonical_payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )

    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()
