from faststream.rabbit import Channel, RabbitBroker

from app.core.config import Settings


def create_rabbit_broker(settings: Settings) -> RabbitBroker:
    return RabbitBroker(
        str(settings.rabbitmq_url),
        default_channel=Channel(
            publisher_confirms=True,
            on_return_raises=True,
        ),
        app_id=settings.app_name,
    )
