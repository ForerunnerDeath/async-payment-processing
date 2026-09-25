from app.models.payment import Payment, PaymentStatus


def test_payment_table_has_expected_columns() -> None:
    columns = Payment.__table__.columns

    assert "id" in columns
    assert "amount" in columns
    assert "currency" in columns
    assert "metadata" in columns
    assert "status" in columns
    assert "idempotency_key" in columns
    assert "request_fingerprint" in columns
    assert "webhook_url" in columns
    assert "webhook_event_id" in columns
    assert "provider_payment_id" in columns
    assert "processed_at" in columns


def test_payment_status_values() -> None:
    assert [status.value for status in PaymentStatus] == [
        "pending",
        "succeeded",
        "failed",
        "unknown",
    ]
