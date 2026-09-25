from faststream.rabbit import RabbitBroker

from app.integrations.rabbitmq.topology import PAYMENTS_EXCHANGE, PAYMENTS_NEW_QUEUE
from app.schemas.event import EventEnvelope


class RabbitEventPublisher:
    def __init__(self, broker: RabbitBroker, *, timeout_seconds: float) -> None:
        self._broker = broker
        self._timeout_seconds = timeout_seconds

    async def publish(self, event: EventEnvelope) -> None:
        await self._broker.publish(
            event.model_dump(mode="json"),
            exchange=PAYMENTS_EXCHANGE,
            routing_key=PAYMENTS_NEW_QUEUE.routing_key,
            mandatory=True,
            persist=True,
            timeout=self._timeout_seconds,
            message_id=str(event.event_id),
            correlation_id=str(event.payment_id),
            message_type=event.event_type.value,
        )
