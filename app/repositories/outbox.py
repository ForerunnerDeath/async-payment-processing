from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.outbox_event import OutboxEvent


class OutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, event: OutboxEvent) -> OutboxEvent:
        self._session.add(event)
        await self._session.flush()

        return event

    async def get_unpublished_batch(self, batch_size: int) -> list[OutboxEvent]:
        if batch_size < 1:
            raise ValueError("batch_size must be at least 1")

        statement = (
            select(OutboxEvent)
            .where(
                OutboxEvent.published_at.is_(None),
            )
            .order_by(
                OutboxEvent.created_at,
                OutboxEvent.id,
            )
            .limit(batch_size)
            .with_for_update(skip_locked=True)
        )

        result = await self._session.execute(statement)

        return list(result.scalars().all())

    @staticmethod
    def mark_published(event: OutboxEvent) -> None:
        event.published_at = datetime.now(UTC)
