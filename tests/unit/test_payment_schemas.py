from decimal import Decimal

import pytest
from pydantic import ValidationError

from app.schemas.payment import Currency, PaymentCreate


def make_payment_data(**overrides: object) -> dict[str, object]:
    data: dict[str, object] = {
        "amount": "1500.50",
        "currency": "RUB",
        "description": "Order 123",
        "metadata": {
            "order_id": "123",
        },
        "webhook_url": "https://example.com/webhook",
    }
    data.update(overrides)

    return data


def test_payment_create_accepts_valid_payload() -> None:
    payment = PaymentCreate.model_validate(
        make_payment_data(),
    )

    assert payment.amount == Decimal("1500.50")
    assert payment.currency is Currency.RUB
    assert payment.description == "Order 123"
    assert payment.metadata == {
        "order_id": "123",
    }
    assert str(payment.webhook_url) == "https://example.com/webhook"


@pytest.mark.parametrize(
    "amount",
    [
        "0",
        "-1.00",
        "1.001",
    ],
)
def test_payment_create_rejects_invalid_amount(
    amount: str,
) -> None:
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate(
            make_payment_data(amount=amount),
        )


def test_payment_create_rejects_unsupported_currency() -> None:
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate(
            make_payment_data(currency="GBP"),
        )


def test_payment_create_rejects_webhook_credentials() -> None:
    with pytest.raises(
        ValidationError,
        match="webhook_url must not contain credentials",
    ):
        PaymentCreate.model_validate(
            make_payment_data(
                webhook_url="https://user:password@example.com/webhook",
            ),
        )


def test_payment_create_uses_independent_metadata_defaults() -> None:
    first = PaymentCreate.model_validate(
        make_payment_data(metadata={}),
    )
    second = PaymentCreate.model_validate(
        make_payment_data(metadata={}),
    )

    first.metadata["payment"] = "first"

    assert second.metadata == {}
