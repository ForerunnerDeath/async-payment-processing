from faststream import AckPolicy
from faststream.rabbit import RabbitBroker, RabbitMessage

from app.integrations.rabbitmq.topology import PAYMENTS_EXCHANGE, PAYMENTS_NEW_QUEUE
from app.worker.consumer import PaymentQueueConsumer


def register_payment_queue_subscriber(broker: RabbitBroker, consumer: PaymentQueueConsumer) -> None:
    subscriber = broker.subscriber(
        PAYMENTS_NEW_QUEUE,
        PAYMENTS_EXCHANGE,
        ack_policy=AckPolicy.MANUAL,
        no_reply=True,
    )

    @subscriber
    async def handle_payment_queue_message(body: object, message: RabbitMessage) -> None:
        await consumer.handle(body, message)
