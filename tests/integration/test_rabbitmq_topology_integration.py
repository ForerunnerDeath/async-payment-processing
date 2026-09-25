import pytest
from faststream.rabbit import RabbitBroker

from app.core.config import get_settings
from app.integrations.rabbitmq.declaration import (
    declare_rabbitmq_topology,
)


@pytest.mark.integration
async def test_rabbitmq_topology_can_be_declared() -> None:
    settings = get_settings()

    broker = RabbitBroker(str(settings.rabbitmq_url))

    try:
        await broker.start()

        await declare_rabbitmq_topology(broker)

        assert await broker.ping(timeout=2.0) is True

    finally:
        await broker.stop()
