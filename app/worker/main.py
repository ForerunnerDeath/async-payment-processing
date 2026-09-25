import asyncio
import signal
from contextlib import suppress
from types import FrameType

import httpx
import structlog

from app.clients.payment_provider import PaymentProviderClient
from app.clients.webhook import WebhookClient
from app.core.config import get_settings
from app.core.database import Database
from app.core.logging import configure_logging
from app.integrations.rabbitmq.broker import create_rabbit_broker
from app.integrations.rabbitmq.declaration import declare_rabbitmq_topology
from app.integrations.rabbitmq.publisher import RabbitEventPublisher
from app.resilience.circuit_breaker import CircuitBreaker
from app.services.outbox_relay import OutboxRelay
from app.services.payment_reconciliation import PaymentReconciliationWorker
from app.worker.consumer import PaymentQueueConsumer
from app.worker.dispatcher import WorkerEventDispatcher
from app.worker.payment_processor import PaymentEventProcessor
from app.worker.subscriber import register_payment_queue_subscriber
from app.worker.webhook_processor import WebhookEventProcessor

logger = structlog.get_logger()

WORKER_NAME = "async-payment-processing-worker"


async def run_worker(shutdown_event: asyncio.Event) -> None:
    settings = get_settings()

    configure_logging(settings.log_level)

    database = Database(settings)

    broker = create_rabbit_broker(settings)

    provider_http_client = httpx.AsyncClient(
        base_url=str(settings.payment_provider_url),
        timeout=settings.payment_provider_request_timeout_seconds,
    )

    webhook_http_client = httpx.AsyncClient()

    publisher = RabbitEventPublisher(
        broker,
        timeout_seconds=settings.rabbit_publish_timeout_seconds,
    )

    provider_client = PaymentProviderClient(
        provider_http_client,
        request_timeout_seconds=(settings.payment_provider_request_timeout_seconds),
        max_attempts=settings.payment_provider_max_attempts,
        retry_base_delay_seconds=(settings.payment_provider_retry_base_delay_seconds),
        retry_max_delay_seconds=(settings.payment_provider_retry_max_delay_seconds),
        retry_total_timeout_seconds=(settings.payment_provider_retry_total_timeout_seconds),
    )

    circuit_breaker = CircuitBreaker(
        failure_threshold=(settings.payment_provider_circuit_breaker_failure_threshold),
        recovery_timeout_seconds=(
            settings.payment_provider_circuit_breaker_recovery_timeout_seconds
        ),
    )

    webhook_client = WebhookClient(
        webhook_http_client,
        timeout_seconds=settings.webhook_request_timeout_seconds,
    )

    payment_processor = PaymentEventProcessor(
        session_factory=database.session_factory,
        provider_client=provider_client,
        circuit_breaker=circuit_breaker,
    )

    webhook_processor = WebhookEventProcessor(
        session_factory=database.session_factory,
        webhook_client=webhook_client,
    )

    dispatcher = WorkerEventDispatcher(
        payment_processor=payment_processor,
        webhook_processor=webhook_processor,
    )

    consumer = PaymentQueueConsumer(
        processor=dispatcher,
        publisher=publisher,
    )

    register_payment_queue_subscriber(
        broker,
        consumer,
    )

    outbox_relay = OutboxRelay(
        session_factory=database.session_factory,
        publisher=publisher,
        batch_size=settings.outbox_relay_batch_size,
        poll_interval_seconds=(settings.outbox_relay_poll_interval_seconds),
    )

    background_stop_event = asyncio.Event()

    outbox_task: asyncio.Task[None] | None = None
    reconciliation_task: asyncio.Task[None] | None = None

    reconciliation_worker = PaymentReconciliationWorker(
        session_factory=database.session_factory,
        provider_client=provider_client,
        circuit_breaker=circuit_breaker,
        batch_size=settings.payment_reconciliation_batch_size,
        stale_after_seconds=(settings.payment_reconciliation_stale_after_seconds),
        poll_interval_seconds=(settings.payment_reconciliation_poll_interval_seconds),
        lease_seconds=settings.payment_reconciliation_lease_seconds,
        max_attempts=settings.payment_reconciliation_max_attempts,
    )

    broker_connected = False
    broker_started = False

    try:
        await database.check_connection()

        await broker.connect()
        broker_connected = True

        await declare_rabbitmq_topology(broker)

        await broker.start()
        broker_started = True

        outbox_task = asyncio.create_task(
            outbox_relay.run(background_stop_event),
            name="outbox-relay",
        )

        reconciliation_task = asyncio.create_task(
            reconciliation_worker.run(background_stop_event),
            name="payment-reconciliation",
        )

        def request_shutdown(_task: asyncio.Task[None]) -> None:
            shutdown_event.set()

        outbox_task.add_done_callback(request_shutdown)
        reconciliation_task.add_done_callback(request_shutdown)

        logger.info(
            "worker_started",
            worker=WORKER_NAME,
        )

        await shutdown_event.wait()

        if outbox_task.done():
            await outbox_task

        if reconciliation_task.done():
            await reconciliation_task

    finally:
        background_stop_event.set()

        if outbox_task is not None and not outbox_task.done():
            await outbox_task

        if reconciliation_task is not None and not reconciliation_task.done():
            await reconciliation_task

        if broker_started or broker_connected:
            with suppress(Exception):
                await broker.stop()

        await provider_http_client.aclose()
        await webhook_http_client.aclose()
        await database.close()

        logger.info(
            "worker_stopped",
            worker=WORKER_NAME,
        )


def install_signal_handlers(shutdown_event: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()

    def handle_signal(_signum: int, _frame: FrameType | None) -> None:
        loop.call_soon_threadsafe(shutdown_event.set)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)


async def worker_main() -> None:
    shutdown_event = asyncio.Event()

    install_signal_handlers(shutdown_event)

    await run_worker(shutdown_event)


def main() -> None:
    asyncio.run(worker_main())


if __name__ == "__main__":
    main()
