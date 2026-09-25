from collections.abc import Awaitable, Callable
from unittest.mock import AsyncMock, MagicMock

from faststream import AckPolicy
from faststream.rabbit import RabbitBroker, RabbitMessage

from app.integrations.rabbitmq.topology import (
    PAYMENTS_EXCHANGE,
    PAYMENTS_NEW_QUEUE,
)
from app.worker.consumer import PaymentQueueConsumer
from app.worker.subscriber import (
    register_payment_queue_subscriber,
)

type MessageHandler = Callable[
    [object, RabbitMessage],
    Awaitable[None],
]


async def test_registers_manual_ack_subscriber_and_delegates_message() -> None:
    broker = MagicMock(
        spec=RabbitBroker,
    )

    registered_handler: MessageHandler | None = None

    def decorator(
        handler: MessageHandler,
    ) -> MessageHandler:
        nonlocal registered_handler

        registered_handler = handler

        return handler

    broker.subscriber.return_value = decorator

    consumer = MagicMock(
        spec=PaymentQueueConsumer,
    )
    consumer.handle = AsyncMock()

    register_payment_queue_subscriber(
        broker,
        consumer,
    )

    broker.subscriber.assert_called_once_with(
        PAYMENTS_NEW_QUEUE,
        PAYMENTS_EXCHANGE,
        ack_policy=AckPolicy.MANUAL,
        no_reply=True,
    )

    assert registered_handler is not None

    body = {
        "event_type": "payment.process_requested",
    }

    message = MagicMock(
        spec=RabbitMessage,
    )

    await registered_handler(
        body,
        message,
    )

    consumer.handle.assert_awaited_once_with(
        body,
        message,
    )
