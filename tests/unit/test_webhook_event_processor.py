from datetime import UTC, datetime
from decimal import Decimal
from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID, uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.clients.webhook import (
    WebhookClient,
    WebhookPermanentError,
    WebhookRetryableError,
)
from app.models.payment import Payment, PaymentStatus
from app.repositories.payment import PaymentRepository
from app.schemas.event import EventEnvelope, EventType
from app.schemas.webhook import WebhookEventType
from app.worker.consumer import (
    PermanentEventError,
    RetryableEventError,
)
from app.worker.webhook_processor import WebhookEventProcessor

PAYMENT_ID = UUID(
    "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
)
WEBHOOK_EVENT_ID = UUID(
    "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
)
PROCESSED_AT = datetime(
    2026,
    9,
    25,
    12,
    0,
    tzinfo=UTC,
)


def make_payment(
    *,
    status: PaymentStatus = PaymentStatus.SUCCEEDED,
    webhook_sent_at: datetime | None = None,
) -> Payment:
    payment = Payment(
        id=PAYMENT_ID,
        amount=Decimal("500.15"),
        currency="RUB",
        description="test payment",
        payment_metadata={
            "order_id": "order-123",
        },
        status=status,
        idempotency_key="client-key",
        request_fingerprint="a" * 64,
        webhook_url="https://client.test/webhook",
        webhook_event_id=WEBHOOK_EVENT_ID,
        webhook_sent_at=webhook_sent_at,
        processed_at=PROCESSED_AT,
    )

    return payment


def make_event(
    *,
    event_id: UUID = WEBHOOK_EVENT_ID,
    attempt: int = 1,
) -> EventEnvelope:
    return EventEnvelope(
        event_id=event_id,
        event_type=EventType.WEBHOOK_DELIVERY_REQUESTED,
        payment_id=PAYMENT_ID,
        attempt=attempt,
        occurred_at=PROCESSED_AT,
    )


def make_session_factory(
    session: AsyncSession,
) -> async_sessionmaker[AsyncSession]:
    session_context = AsyncMock()
    session_context.__aenter__.return_value = session

    factory = MagicMock(
        return_value=session_context,
    )

    return cast(
        async_sessionmaker[AsyncSession],
        factory,
    )


async def test_successful_webhook_delivery_marks_payment_sent() -> None:
    payment = make_payment()

    session = AsyncMock(spec=AsyncSession)
    session_factory = make_session_factory(session)

    read_repository = MagicMock(spec=PaymentRepository)
    read_repository.get_by_id = AsyncMock(
        return_value=payment,
    )

    locked_repository = MagicMock(spec=PaymentRepository)
    locked_repository.get_by_id_for_update = AsyncMock(
        return_value=payment,
    )

    webhook_client = MagicMock(spec=WebhookClient)
    webhook_client.deliver = AsyncMock()

    processor = WebhookEventProcessor(
        session_factory=session_factory,
        webhook_client=webhook_client,
    )

    with patch(
        "app.worker.webhook_processor.PaymentRepository",
        side_effect=[
            read_repository,
            locked_repository,
        ],
    ):
        await processor.process(
            make_event(),
        )

    webhook_client.deliver.assert_awaited_once()

    call = webhook_client.deliver.await_args

    assert call.kwargs["url"] == "https://client.test/webhook"
    assert call.kwargs["event_id"] == WEBHOOK_EVENT_ID
    assert call.kwargs["attempt"] == 1

    payload = call.kwargs["payload"]

    assert payload.event_id == WEBHOOK_EVENT_ID
    assert payload.payment_id == PAYMENT_ID
    assert payload.event_type == WebhookEventType.PAYMENT_SUCCEEDED
    assert payload.status == PaymentStatus.SUCCEEDED
    assert payload.amount == Decimal("500.15")
    assert payload.metadata == {
        "order_id": "order-123",
    }

    assert payment.webhook_sent_at is not None
    session.commit.assert_awaited_once_with()


async def test_failed_payment_uses_failed_webhook_event_type() -> None:
    payment = make_payment(
        status=PaymentStatus.FAILED,
    )

    session = AsyncMock(spec=AsyncSession)
    session_factory = make_session_factory(session)

    read_repository = MagicMock(spec=PaymentRepository)
    read_repository.get_by_id = AsyncMock(
        return_value=payment,
    )

    locked_repository = MagicMock(spec=PaymentRepository)
    locked_repository.get_by_id_for_update = AsyncMock(
        return_value=payment,
    )

    webhook_client = MagicMock(spec=WebhookClient)
    webhook_client.deliver = AsyncMock()

    processor = WebhookEventProcessor(
        session_factory,
        webhook_client,
    )

    with patch(
        "app.worker.webhook_processor.PaymentRepository",
        side_effect=[
            read_repository,
            locked_repository,
        ],
    ):
        await processor.process(
            make_event(),
        )

    payload = webhook_client.deliver.await_args.kwargs["payload"]

    assert payload.event_type == WebhookEventType.PAYMENT_FAILED
    assert payload.status == PaymentStatus.FAILED


async def test_already_sent_webhook_is_idempotent_noop() -> None:
    payment = make_payment(
        webhook_sent_at=datetime.now(UTC),
    )

    session = AsyncMock(spec=AsyncSession)
    session_factory = make_session_factory(session)

    repository = MagicMock(spec=PaymentRepository)
    repository.get_by_id = AsyncMock(
        return_value=payment,
    )

    webhook_client = MagicMock(spec=WebhookClient)
    webhook_client.deliver = AsyncMock()

    processor = WebhookEventProcessor(
        session_factory,
        webhook_client,
    )

    with patch(
        "app.worker.webhook_processor.PaymentRepository",
        return_value=repository,
    ):
        await processor.process(
            make_event(),
        )

    webhook_client.deliver.assert_not_awaited()
    session.commit.assert_not_awaited()


async def test_retryable_webhook_error_becomes_retryable_event_error() -> None:
    payment = make_payment()

    session = AsyncMock(spec=AsyncSession)
    session_factory = make_session_factory(session)

    repository = MagicMock(spec=PaymentRepository)
    repository.get_by_id = AsyncMock(
        return_value=payment,
    )

    webhook_client = MagicMock(spec=WebhookClient)
    webhook_client.deliver = AsyncMock(
        side_effect=WebhookRetryableError(
            "temporary failure",
            status_code=503,
        ),
    )

    processor = WebhookEventProcessor(
        session_factory,
        webhook_client,
    )

    with (
        patch(
            "app.worker.webhook_processor.PaymentRepository",
            return_value=repository,
        ),
        pytest.raises(
            RetryableEventError,
            match="retryable error",
        ),
    ):
        await processor.process(
            make_event(
                attempt=2,
            )
        )

    assert payment.webhook_sent_at is None


async def test_permanent_webhook_error_becomes_permanent_event_error() -> None:
    payment = make_payment()

    session = AsyncMock(spec=AsyncSession)
    session_factory = make_session_factory(session)

    repository = MagicMock(spec=PaymentRepository)
    repository.get_by_id = AsyncMock(
        return_value=payment,
    )

    webhook_client = MagicMock(spec=WebhookClient)
    webhook_client.deliver = AsyncMock(
        side_effect=WebhookPermanentError(
            "bad webhook",
            status_code=400,
        ),
    )

    processor = WebhookEventProcessor(
        session_factory,
        webhook_client,
    )

    with (
        patch(
            "app.worker.webhook_processor.PaymentRepository",
            return_value=repository,
        ),
        pytest.raises(
            PermanentEventError,
            match="failed permanently",
        ),
    ):
        await processor.process(
            make_event(),
        )

    assert payment.webhook_sent_at is None


async def test_mismatched_webhook_event_id_is_permanent_error() -> None:
    payment = make_payment()

    session = AsyncMock(spec=AsyncSession)
    session_factory = make_session_factory(session)

    repository = MagicMock(spec=PaymentRepository)
    repository.get_by_id = AsyncMock(
        return_value=payment,
    )

    webhook_client = MagicMock(spec=WebhookClient)

    processor = WebhookEventProcessor(
        session_factory,
        webhook_client,
    )

    with (
        patch(
            "app.worker.webhook_processor.PaymentRepository",
            return_value=repository,
        ),
        pytest.raises(
            PermanentEventError,
            match="event_id does not match",
        ),
    ):
        await processor.process(
            make_event(
                event_id=uuid4(),
            )
        )


@pytest.mark.parametrize(
    "payment_status",
    [
        PaymentStatus.PENDING,
        PaymentStatus.UNKNOWN,
    ],
)
async def test_non_terminal_payment_cannot_send_webhook(
    payment_status: PaymentStatus,
) -> None:
    payment = make_payment(
        status=payment_status,
    )

    session = AsyncMock(spec=AsyncSession)
    session_factory = make_session_factory(session)

    repository = MagicMock(spec=PaymentRepository)
    repository.get_by_id = AsyncMock(
        return_value=payment,
    )

    webhook_client = MagicMock(spec=WebhookClient)

    processor = WebhookEventProcessor(
        session_factory,
        webhook_client,
    )

    with (
        patch(
            "app.worker.webhook_processor.PaymentRepository",
            return_value=repository,
        ),
        pytest.raises(
            PermanentEventError,
            match="non-terminal",
        ),
    ):
        await processor.process(
            make_event(),
        )


async def test_missing_payment_is_permanent_error() -> None:
    session = AsyncMock(spec=AsyncSession)
    session_factory = make_session_factory(session)

    repository = MagicMock(spec=PaymentRepository)
    repository.get_by_id = AsyncMock(
        return_value=None,
    )

    webhook_client = MagicMock(spec=WebhookClient)

    processor = WebhookEventProcessor(
        session_factory,
        webhook_client,
    )

    with (
        patch(
            "app.worker.webhook_processor.PaymentRepository",
            return_value=repository,
        ),
        pytest.raises(
            PermanentEventError,
            match="was not found",
        ),
    ):
        await processor.process(
            make_event(),
        )


async def test_concurrent_consumer_that_already_marked_sent_wins() -> None:
    payment_before_http = make_payment()

    payment_after_http = make_payment(
        webhook_sent_at=datetime.now(UTC),
    )

    session = AsyncMock(spec=AsyncSession)
    session_factory = make_session_factory(session)

    read_repository = MagicMock(spec=PaymentRepository)
    read_repository.get_by_id = AsyncMock(
        return_value=payment_before_http,
    )

    locked_repository = MagicMock(spec=PaymentRepository)
    locked_repository.get_by_id_for_update = AsyncMock(
        return_value=payment_after_http,
    )

    webhook_client = MagicMock(spec=WebhookClient)
    webhook_client.deliver = AsyncMock()

    processor = WebhookEventProcessor(
        session_factory,
        webhook_client,
    )

    with patch(
        "app.worker.webhook_processor.PaymentRepository",
        side_effect=[
            read_repository,
            locked_repository,
        ],
    ):
        await processor.process(
            make_event(),
        )

    webhook_client.deliver.assert_awaited_once()
    session.commit.assert_not_awaited()
