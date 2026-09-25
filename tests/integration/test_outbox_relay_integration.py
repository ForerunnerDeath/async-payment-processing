import json
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import (
    async_sessionmaker,
    create_async_engine,
)

from app.core.config import get_settings
from app.integrations.rabbitmq.broker import create_rabbit_broker
from app.integrations.rabbitmq.declaration import (
    declare_rabbitmq_topology,
)
from app.integrations.rabbitmq.publisher import RabbitEventPublisher
from app.integrations.rabbitmq.topology import PAYMENTS_NEW_QUEUE
from app.models.outbox_event import OutboxEvent
from app.schemas.event import EventEnvelope, EventType
from app.services.outbox_relay import OutboxRelay


@pytest.mark.integration
async def test_outbox_relay_publishes_database_event_to_rabbitmq() -> None:
    settings = get_settings()

    engine = create_async_engine(
        str(settings.database_url),
        pool_pre_ping=True,
    )

    session_factory = async_sessionmaker(
        bind=engine,
        expire_on_commit=False,
    )

    broker = create_rabbit_broker(settings)

    event = EventEnvelope(
        event_id=uuid4(),
        event_type=EventType.PAYMENT_PROCESS_REQUESTED,
        payment_id=uuid4(),
        occurred_at=datetime.now(UTC),
    )

    outbox_event = OutboxEvent(
        id=event.event_id,
        event_type=event.event_type.value,
        payment_id=event.payment_id,
        payload=event.model_dump(mode="json"),
        created_at=datetime(2000, 1, 1, tzinfo=UTC),
    )

    message = None

    try:
        await broker.start()
        await declare_rabbitmq_topology(broker)

        queue = await broker.declare_queue(
            PAYMENTS_NEW_QUEUE,
        )
        await queue.purge()

        async with session_factory() as session:
            session.add(outbox_event)
            await session.commit()

        publisher = RabbitEventPublisher(
            broker,
            timeout_seconds=settings.rabbit_publish_timeout_seconds,
        )

        relay = OutboxRelay(
            session_factory=session_factory,
            publisher=publisher,
            batch_size=1,
            poll_interval_seconds=1.0,
        )

        processed_count = await relay.process_batch()

        assert processed_count == 1

        message = await queue.get(
            timeout=2.0,
            fail=False,
        )

        assert message is not None

        payload = json.loads(message.body)

        assert payload["event_id"] == str(event.event_id)
        assert payload["payment_id"] == str(event.payment_id)
        assert payload["event_type"] == "payment.process_requested"

        async with session_factory() as session:
            stored_event = await session.get(
                OutboxEvent,
                event.event_id,
            )

            assert stored_event is not None
            assert stored_event.published_at is not None
    finally:
        if message is not None:
            await message.ack()

        async with session_factory() as session:
            await session.execute(
                delete(OutboxEvent).where(
                    OutboxEvent.id == event.event_id,
                )
            )
            await session.commit()

        await broker.stop()
        await engine.dispose()
