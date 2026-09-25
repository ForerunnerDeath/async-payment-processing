from faststream.rabbit import ExchangeType, RabbitExchange, RabbitQueue

PAYMENTS_EXCHANGE = RabbitExchange(
    name="payments",
    type=ExchangeType.DIRECT,
    durable=True,
)

PAYMENTS_DLX = RabbitExchange(
    name="payments.dlx",
    type=ExchangeType.DIRECT,
    durable=True,
)

PAYMENTS_NEW_QUEUE = RabbitQueue(
    name="payments.new",
    durable=True,
    routing_key="payments.new",
    arguments={
        "x-dead-letter-exchange": PAYMENTS_DLX.name,
        "x-dead-letter-routing-key": "payments.dlq",
    },
)

PAYMENTS_RETRY_1_QUEUE = RabbitQueue(
    name="payments.retry.1",
    durable=True,
    routing_key="payments.retry.1",
    arguments={
        "x-message-ttl": 1000,
        "x-dead-letter-exchange": PAYMENTS_EXCHANGE.name,
        "x-dead-letter-routing-key": PAYMENTS_NEW_QUEUE.routing_key,
    },
)

PAYMENTS_RETRY_2_QUEUE = RabbitQueue(
    name="payments.retry.2",
    durable=True,
    routing_key="payments.retry.2",
    arguments={
        "x-message-ttl": 2000,
        "x-dead-letter-exchange": PAYMENTS_EXCHANGE.name,
        "x-dead-letter-routing-key": PAYMENTS_NEW_QUEUE.routing_key,
    },
)

PAYMENTS_DLQ = RabbitQueue(
    name="payments.dlq",
    durable=True,
    routing_key="payments.dlq",
)
