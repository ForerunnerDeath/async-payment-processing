import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.core.config import Settings
from app.worker.main import WORKER_NAME, run_worker


async def test_run_worker_starts_and_stops_resources(
    settings: Settings,
) -> None:
    shutdown_event = asyncio.Event()
    order: list[str] = []

    async def check_database() -> None:
        order.append("database_checked")

    async def start_broker() -> None:
        order.append("broker_started")

    async def declare_topology(_: object) -> None:
        order.append("topology_declared")

    async def run_relay(
        stop_event: asyncio.Event,
    ) -> None:
        order.append("relay_started")

        shutdown_event.set()

        await stop_event.wait()

        order.append("relay_stopped")

    async def stop_broker() -> None:
        order.append("broker_stopped")

    async def close_database() -> None:
        order.append("database_closed")

    database = MagicMock()
    database.session_factory = MagicMock()
    database.check_connection = AsyncMock(
        side_effect=check_database,
    )
    database.close = AsyncMock(
        side_effect=close_database,
    )

    broker = MagicMock()
    broker.start = AsyncMock(
        side_effect=start_broker,
    )
    broker.stop = AsyncMock(
        side_effect=stop_broker,
    )

    publisher = MagicMock()

    relay = MagicMock()
    relay.run = AsyncMock(
        side_effect=run_relay,
    )

    logger = MagicMock()

    with (
        patch(
            "app.worker.main.get_settings",
            return_value=settings,
        ),
        patch(
            "app.worker.main.configure_logging",
        ) as configure_logging,
        patch(
            "app.worker.main.Database",
            return_value=database,
        ) as database_class,
        patch(
            "app.worker.main.create_rabbit_broker",
            return_value=broker,
        ) as create_broker,
        patch(
            "app.worker.main.declare_rabbitmq_topology",
            new=AsyncMock(
                side_effect=declare_topology,
            ),
        ) as declare,
        patch(
            "app.worker.main.RabbitEventPublisher",
            return_value=publisher,
        ) as publisher_class,
        patch(
            "app.worker.main.OutboxRelay",
            return_value=relay,
        ) as relay_class,
        patch(
            "app.worker.main.structlog.get_logger",
            return_value=logger,
        ),
    ):
        await run_worker(shutdown_event)

    assert order == [
        "database_checked",
        "broker_started",
        "topology_declared",
        "relay_started",
        "relay_stopped",
        "broker_stopped",
        "database_closed",
    ]

    configure_logging.assert_called_once_with(
        settings.log_level,
    )

    database_class.assert_called_once_with(settings)
    create_broker.assert_called_once_with(settings)

    publisher_class.assert_called_once_with(
        broker,
        timeout_seconds=settings.rabbit_publish_timeout_seconds,
    )

    relay_class.assert_called_once_with(
        session_factory=database.session_factory,
        publisher=publisher,
        batch_size=settings.outbox_relay_batch_size,
        poll_interval_seconds=settings.outbox_relay_poll_interval_seconds,
    )

    declare.assert_awaited_once_with(broker)

    relay.run.assert_awaited_once()

    broker.stop.assert_awaited_once_with()
    database.close.assert_awaited_once_with()

    logger.info.assert_any_call(
        "worker_started",
        worker=WORKER_NAME,
        environment=settings.environment,
    )

    logger.info.assert_any_call(
        "worker_stopped",
        worker=WORKER_NAME,
    )


async def test_run_worker_closes_database_when_broker_start_fails(
    settings: Settings,
) -> None:
    shutdown_event = asyncio.Event()

    database = MagicMock()
    database.session_factory = MagicMock()
    database.check_connection = AsyncMock()
    database.close = AsyncMock()

    broker = MagicMock()
    broker.start = AsyncMock(
        side_effect=RuntimeError("RabbitMQ unavailable"),
    )
    broker.stop = AsyncMock()

    with (
        patch(
            "app.worker.main.get_settings",
            return_value=settings,
        ),
        patch("app.worker.main.configure_logging"),
        patch(
            "app.worker.main.Database",
            return_value=database,
        ),
        patch(
            "app.worker.main.create_rabbit_broker",
            return_value=broker,
        ),
        patch("app.worker.main.RabbitEventPublisher"),
        patch("app.worker.main.OutboxRelay"),
        patch("app.worker.main.structlog.get_logger"),
        pytest.raises(
            RuntimeError,
            match="RabbitMQ unavailable",
        ),
    ):
        await run_worker(shutdown_event)

    broker.stop.assert_not_awaited()
    database.close.assert_awaited_once_with()
