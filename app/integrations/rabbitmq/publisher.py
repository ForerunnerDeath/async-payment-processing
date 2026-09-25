from faststream.rabbit import RabbitBroker

from app.integrations.rabbitmq.topology import (
    PAYMENTS_EXCHANGE,
    PAYMENTS_NEW_QUEUE,
    PAYMENTS_RETRY_1_QUEUE,
    PAYMENTS_RETRY_2_QUEUE,
)
from app.schemas.event import EventEnvelope


class RabbitEventPublisher:
    def __init__(self, broker: RabbitBroker, *, timeout_seconds: float) -> None:
        self._broker = broker
        self._timeout_seconds = timeout_seconds

    async def publish(self, event: EventEnvelope) -> None:
        await self._publish(
            event,
            routing_key=PAYMENTS_NEW_QUEUE.routing_key,
        )

    async def publish_retry(self, event: EventEnvelope) -> None:
        if event.attempt == 2:
            routing_key = PAYMENTS_RETRY_1_QUEUE.routing_key
        elif event.attempt == 3:
            routing_key = PAYMENTS_RETRY_2_QUEUE.routing_key
        else:
            raise ValueError("Retry event attempt must be 2 or 3")

        await self._publish(event, routing_key=routing_key)

    async def _publish(self, event: EventEnvelope, *, routing_key: str) -> None:
        await self._broker.publish(
            event.model_dump(mode="json"),
            exchange=PAYMENTS_EXCHANGE,
            routing_key=routing_key,
            mandatory=True,
            persist=True,
            timeout=self._timeout_seconds,
            message_id=str(event.event_id),
            correlation_id=str(event.payment_id),
            message_type=event.event_type.value,
        )
