from app.schemas.payment import PaymentCreate
from app.services.idempotency import build_request_fingerprint


def make_payment(
    **overrides: object,
) -> PaymentCreate:
    data: dict[str, object] = {
        "amount": "1500.50",
        "currency": "RUB",
        "description": "Order 123",
        "metadata": {
            "order_id": "123",
            "customer": "ivan",
        },
        "webhook_url": "https://example.com/webhook",
    }
    data.update(overrides)

    return PaymentCreate.model_validate(data)


def test_fingerprint_is_deterministic() -> None:
    payment = make_payment()

    first = build_request_fingerprint(payment)
    second = build_request_fingerprint(payment)

    assert first == second
    assert len(first) == 64


def test_fingerprint_ignores_metadata_key_order() -> None:
    first = make_payment(
        metadata={
            "order_id": "123",
            "customer": "ivan",
        }
    )
    second = make_payment(
        metadata={
            "customer": "ivan",
            "order_id": "123",
        }
    )

    assert build_request_fingerprint(first) == build_request_fingerprint(second)


def test_fingerprint_normalizes_amount_scale() -> None:
    first = make_payment(amount="1500.5")
    second = make_payment(amount="1500.50")

    assert build_request_fingerprint(first) == build_request_fingerprint(second)


def test_fingerprint_changes_when_request_changes() -> None:
    original = make_payment()
    changed = make_payment(
        webhook_url="https://example.com/another-webhook",
    )

    assert build_request_fingerprint(original) != build_request_fingerprint(changed)
