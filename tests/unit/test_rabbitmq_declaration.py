from unittest.mock import AsyncMock, MagicMock, call

from faststream.rabbit import RabbitBroker

from app.integrations.rabbitmq.declaration import (
    declare_rabbitmq_topology,
)
from app.integrations.rabbitmq.topology import (
    PAYMENTS_DLQ,
    PAYMENTS_DLX,
    PAYMENTS_EXCHANGE,
    PAYMENTS_NEW_QUEUE,
    PAYMENTS_RETRY_1_QUEUE,
    PAYMENTS_RETRY_2_QUEUE,
)


async def test_declare_rabbitmq_topology_declares_and_binds_everything() -> None:
    broker = MagicMock(spec=RabbitBroker)

    payments_exchange = MagicMock()
    payments_dlx = MagicMock()

    payments_new_queue = MagicMock()
    payments_new_queue.bind = AsyncMock()

    payments_retry_1_queue = MagicMock()
    payments_retry_1_queue.bind = AsyncMock()

    payments_retry_2_queue = MagicMock()
    payments_retry_2_queue.bind = AsyncMock()

    payments_dlq = MagicMock()
    payments_dlq.bind = AsyncMock()

    broker.declare_exchange = AsyncMock(
        side_effect=[
            payments_exchange,
            payments_dlx,
        ]
    )

    broker.declare_queue = AsyncMock(
        side_effect=[
            payments_new_queue,
            payments_retry_1_queue,
            payments_retry_2_queue,
            payments_dlq,
        ]
    )

    await declare_rabbitmq_topology(broker)

    assert broker.declare_exchange.await_args_list == [
        call(PAYMENTS_EXCHANGE),
        call(PAYMENTS_DLX),
    ]

    assert broker.declare_queue.await_args_list == [
        call(PAYMENTS_NEW_QUEUE),
        call(PAYMENTS_RETRY_1_QUEUE),
        call(PAYMENTS_RETRY_2_QUEUE),
        call(PAYMENTS_DLQ),
    ]

    payments_new_queue.bind.assert_awaited_once_with(
        payments_exchange,
        routing_key="payments.new",
    )

    payments_retry_1_queue.bind.assert_awaited_once_with(
        payments_exchange,
        routing_key="payments.retry.1",
    )

    payments_retry_2_queue.bind.assert_awaited_once_with(
        payments_exchange,
        routing_key="payments.retry.2",
    )

    payments_dlq.bind.assert_awaited_once_with(
        payments_dlx,
        routing_key="payments.dlq",
    )
