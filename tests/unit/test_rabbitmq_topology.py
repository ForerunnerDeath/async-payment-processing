from faststream.rabbit import ExchangeType

from app.integrations.rabbitmq.topology import (
    PAYMENTS_DLQ,
    PAYMENTS_DLX,
    PAYMENTS_EXCHANGE,
    PAYMENTS_NEW_QUEUE,
    PAYMENTS_RETRY_1_QUEUE,
    PAYMENTS_RETRY_2_QUEUE,
)


def test_rabbitmq_exchanges_are_durable_direct_exchanges() -> None:
    assert PAYMENTS_EXCHANGE.name == "payments"
    assert PAYMENTS_EXCHANGE.type is ExchangeType.DIRECT
    assert PAYMENTS_EXCHANGE.durable is True

    assert PAYMENTS_DLX.name == "payments.dlx"
    assert PAYMENTS_DLX.type is ExchangeType.DIRECT
    assert PAYMENTS_DLX.durable is True


def test_main_queue_dead_letters_to_dlq() -> None:
    assert PAYMENTS_NEW_QUEUE.name == "payments.new"
    assert PAYMENTS_NEW_QUEUE.routing_key == "payments.new"
    assert PAYMENTS_NEW_QUEUE.durable is True

    assert PAYMENTS_NEW_QUEUE.arguments["x-dead-letter-exchange"] == "payments.dlx"
    assert PAYMENTS_NEW_QUEUE.arguments["x-dead-letter-routing-key"] == "payments.dlq"


def test_retry_queues_return_messages_to_main_queue_after_ttl() -> None:
    assert PAYMENTS_RETRY_1_QUEUE.arguments["x-message-ttl"] == 1000
    assert PAYMENTS_RETRY_1_QUEUE.arguments["x-dead-letter-exchange"] == "payments"
    assert PAYMENTS_RETRY_1_QUEUE.arguments["x-dead-letter-routing-key"] == "payments.new"

    assert PAYMENTS_RETRY_2_QUEUE.arguments["x-message-ttl"] == 2000
    assert PAYMENTS_RETRY_2_QUEUE.arguments["x-dead-letter-exchange"] == "payments"
    assert PAYMENTS_RETRY_2_QUEUE.arguments["x-dead-letter-routing-key"] == "payments.new"


def test_dlq_is_durable() -> None:
    assert PAYMENTS_DLQ.name == "payments.dlq"
    assert PAYMENTS_DLQ.routing_key == "payments.dlq"
    assert PAYMENTS_DLQ.durable is True
