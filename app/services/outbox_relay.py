import asyncio
from contextlib import suppress

import structlog
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.integrations.rabbitmq.publisher import RabbitEventPublisher
from app.repositories.outbox import OutboxRepository
from app.schemas.event import EventEnvelope


class OutboxRelay:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        publisher: RabbitEventPublisher,
        *,
        batch_size: int,
        poll_interval_seconds: float,
    ) -> None:
        self._session_factory = session_factory
        self._publisher = publisher
        self._batch_size = batch_size
        self._poll_interval_seconds = poll_interval_seconds

    async def process_batch(self) -> int:
        async with self._session_factory() as session:
            repository = OutboxRepository(session)

            try:
                events = await repository.get_unpublished_batch(self._batch_size)

                for event in events:
                    envelope = EventEnvelope.model_validate(event.payload)

                    await self._publisher.publish(envelope)

                    repository.mark_published(event)

                await session.commit()

                return len(events)

            except Exception:
                await session.rollback()
                raise

    async def run(self, stop_event: asyncio.Event) -> None:
        logger = structlog.get_logger()

        logger.info("outbox_relay_started")

        try:
            while not stop_event.is_set():
                try:
                    processed_count = await self.process_batch()

                    if processed_count > 0:
                        logger.info(
                            "outbox_batch_published",
                            count=processed_count,
                        )

                except Exception:
                    logger.exception("outbox_batch_failed")

                with suppress(TimeoutError):
                    await asyncio.wait_for(
                        stop_event.wait(),
                        timeout=self._poll_interval_seconds,
                    )
        finally:
            logger.info("outbox_relay_stopped")
