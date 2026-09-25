from unittest.mock import AsyncMock, MagicMock

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
