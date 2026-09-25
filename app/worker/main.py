import asyncio
import signal
from types import FrameType

import structlog

from app.core.config import get_settings
from app.core.database import Database
from app.core.logging import configure_logging
from app.integrations.rabbitmq import (
    RabbitEventPublisher,
    create_rabbit_broker,
    declare_rabbitmq_topology,
)
from app.services.outbox_relay import OutboxRelay

WORKER_NAME = "async-payment-processing-worker"


async def run_worker(shutdown_event: asyncio.Event) -> None:
    settings = get_settings()

    configure_logging(settings.log_level)

    logger = structlog.get_logger()

    database = Database(settings)
    broker = create_rabbit_broker(settings)

    publisher = RabbitEventPublisher(
        broker,
        timeout_seconds=settings.rabbit_publish_timeout_seconds,
    )

    outbox_relay = OutboxRelay(
        session_factory=database.session_factory,
        publisher=publisher,
        batch_size=settings.outbox_relay_batch_size,
        poll_interval_seconds=settings.outbox_relay_poll_interval_seconds,
    )

    outbox_stop_event = asyncio.Event()
    outbox_task: asyncio.Task[None] | None = None
    broker_started = False

    try:
        await database.check_connection()

        await broker.start()
        broker_started = True

        await declare_rabbitmq_topology(broker)

        outbox_task = asyncio.create_task(
            outbox_relay.run(outbox_stop_event),
            name="outbox_relay",
        )

        outbox_task.add_done_callback(lambda _task: shutdown_event.set())

        logger.info(
            "worker_started",
            worker=WORKER_NAME,
            environment=settings.environment,
        )

        await shutdown_event.wait()

        if outbox_task.done():
            await outbox_task

    finally:
        outbox_stop_event.set()

        try:
            if outbox_task is not None and not outbox_task.done():
                await outbox_task
        finally:
            try:
                if broker_started:
                    await broker.stop()
            finally:
                await database.close()

                logger.info(
                    "worker_stopped",
                    worker=WORKER_NAME,
                )


def install_signal_handlers(shutdown_event: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()

    def handle_shutdown(_signum: int, _frame: FrameType | None) -> None:
        loop.call_soon_threadsafe(shutdown_event.set)

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)


async def worker_main() -> None:
    shutdown_event = asyncio.Event()

    install_signal_handlers(shutdown_event)

    await run_worker(shutdown_event)


def main() -> None:
    asyncio.run(worker_main())


if __name__ == "__main__":
    main()
