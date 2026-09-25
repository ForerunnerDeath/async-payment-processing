from faststream.rabbit import RabbitBroker

from app.integrations.rabbitmq.topology import (
    PAYMENTS_DLQ,
    PAYMENTS_DLX,
    PAYMENTS_EXCHANGE,
    PAYMENTS_NEW_QUEUE,
    PAYMENTS_RETRY_1_QUEUE,
    PAYMENTS_RETRY_2_QUEUE,
)


async def declare_rabbitmq_topology(broker: RabbitBroker) -> None:
    payments_exchange = await broker.declare_exchange(PAYMENTS_EXCHANGE)
    payments_dlx = await broker.declare_exchange(PAYMENTS_DLX)

    payments_new_queue = await broker.declare_queue(PAYMENTS_NEW_QUEUE)
    payments_retry_1_queue = await broker.declare_queue(PAYMENTS_RETRY_1_QUEUE)
    payments_retry_2_queue = await broker.declare_queue(PAYMENTS_RETRY_2_QUEUE)
    payments_dlq = await broker.declare_queue(PAYMENTS_DLQ)

    await payments_new_queue.bind(
        payments_exchange,
        routing_key=PAYMENTS_NEW_QUEUE.routing_key,
    )

    await payments_retry_1_queue.bind(
        payments_exchange,
        routing_key=PAYMENTS_RETRY_1_QUEUE.routing_key,
    )

    await payments_retry_2_queue.bind(
        payments_exchange,
        routing_key=PAYMENTS_RETRY_2_QUEUE.routing_key,
    )

    await payments_dlq.bind(
        payments_dlx,
        routing_key=PAYMENTS_DLQ.routing_key,
    )
