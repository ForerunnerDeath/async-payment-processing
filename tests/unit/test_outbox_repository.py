from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.outbox_event import OutboxEvent
from app.repositories.outbox import OutboxRepository


async def test_add_outbox_event_flushes_session() -> None:
    event = MagicMock(spec=OutboxEvent)

    session = MagicMock(spec=AsyncSession)
    session.flush = AsyncMock()

    repository = OutboxRepository(session)

    returned_event = await repository.add(event)

    assert returned_event is event
    session.add.assert_called_once_with(event)
    session.flush.assert_awaited_once_with()


async def test_get_unpublished_batch_rejects_invalid_batch_size() -> None:
    session = MagicMock(spec=AsyncSession)
    session.execute = AsyncMock()

    repository = OutboxRepository(session)

    with pytest.raises(
        ValueError,
        match="batch_size must be at least 1",
    ):
        await repository.get_unpublished_batch(0)

    session.execute.assert_not_awaited()


async def test_get_unpublished_batch_executes_locked_query() -> None:
    event_1 = OutboxEvent(
        id=uuid4(),
        event_type="payment.process_requested",
        payment_id=uuid4(),
        payload={},
    )
    event_2 = OutboxEvent(
        id=uuid4(),
        event_type="payment.process_requested",
        payment_id=uuid4(),
        payload={},
    )

    scalar_result = MagicMock()
    scalar_result.all.return_value = [
        event_1,
        event_2,
    ]

    execute_result = MagicMock()
    execute_result.scalars.return_value = scalar_result

    session = MagicMock(spec=AsyncSession)
    session.execute = AsyncMock(
        return_value=execute_result,
    )

    repository = OutboxRepository(session)

    events = await repository.get_unpublished_batch(
        batch_size=2,
    )

    assert events == [
        event_1,
        event_2,
    ]

    session.execute.assert_awaited_once()

    statement = session.execute.await_args.args[0]

    compiled_sql = str(
        statement.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={
                "literal_binds": True,
            },
        )
    )

    normalized_sql = " ".join(
        compiled_sql.split(),
    ).upper()

    assert "WHERE OUTBOX_EVENTS.PUBLISHED_AT IS NULL" in normalized_sql
    assert "ORDER BY OUTBOX_EVENTS.CREATED_AT, OUTBOX_EVENTS.ID" in normalized_sql
    assert "LIMIT 2" in normalized_sql
    assert "FOR UPDATE SKIP LOCKED" in normalized_sql


def test_mark_published_sets_timestamp() -> None:
    event = OutboxEvent(
        id=uuid4(),
        event_type="payment.process_requested",
        payment_id=uuid4(),
        payload={},
    )

    before = datetime.now(UTC)

    OutboxRepository.mark_published(event)

    after = datetime.now(UTC)

    assert event.published_at is not None
    assert before <= event.published_at <= after
