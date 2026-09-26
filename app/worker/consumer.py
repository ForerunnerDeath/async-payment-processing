from typing import Protocol

import structlog
from faststream.rabbit import RabbitMessage
from pydantic import ValidationError

from app.integrations.rabbitmq.publisher import RabbitEventPublisher
from app.schemas.event import EventEnvelope


class RetryableEventError(Exception):
    pass


class PermanentEventError(Exception):
    pass


class EventProcessor(Protocol):
    async def process(self, event: EventEnvelope) -> None: ...


class PaymentQueueConsumer:
    MAX_ATTEMPTS = 3

    def __init__(self, processor: EventProcessor, publisher: RabbitEventPublisher) -> None:
        self._processor = processor
        self._publisher = publisher
        self._logger = structlog.get_logger()

    async def handle(self, body: object, message: RabbitMessage) -> None:
        try:
            event = EventEnvelope.model_validate(body)
        except ValidationError:
            self._logger.exception("rabbit_message_invalid")

            await message.reject()

            return

        try:
            await self._processor.process(event)
        except PermanentEventError:
            self._logger.exception(
                "rabbit_event_permanent_failure",
                event_id=str(event.event_id),
                event_type=event.event_type.value,
                payment_id=str(event.payment_id),
                attempt=event.attempt,
            )

            await message.reject()

            return
        except RetryableEventError:
            self._logger.exception(
                "rabbit_event_retryable_failure",
                event_id=str(event.event_id),
                event_type=event.event_type.value,
                payment_id=str(event.payment_id),
                attempt=event.attempt,
            )
            await self._retry_or_reject(event, message)

            return

        except Exception:
            self._logger.exception(
                "rabbit_event_unexpected_failure",
                event_id=str(event.event_id),
                event_type=event.event_type.value,
                payment_id=str(event.payment_id),
                attempt=event.attempt,
            )

            await self._retry_or_reject(event, message)

            return

        await message.ack()

    async def _retry_or_reject(self, event: EventEnvelope, message: RabbitMessage) -> None:
        if event.attempt >= self.MAX_ATTEMPTS:
            await message.reject()

            return

        retry_event = event.model_copy(update={"attempt": event.attempt + 1})

        try:
            await self._publisher.publish_retry(retry_event)
        except Exception:
            self._logger.exception(
                "rabbit_retry_publish_failed",
                event_id=str(event.event_id),
                event_type=event.event_type.value,
                payment_id=str(event.payment_id),
                attempt=event.attempt,
                next_attempt=retry_event.attempt,
            )

            await message.nack()

            return

        await message.ack()
