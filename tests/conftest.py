import pytest

from app.core.config import Settings


@pytest.fixture
def settings() -> Settings:
    return Settings.model_validate(
        {
            "api_key": "test-api-key",
            "database_url": ("postgresql+asyncpg://payments:payments@localhost:5432/payments"),
            "rabbitmq_url": "amqp://payments:payments@localhost:5672/",
            "payment_provider_url": "http://localhost:8001",
            "payment_provider_request_timeout_seconds": 6.0,
            "payment_provider_max_attempts": 3,
            "payment_provider_retry_base_delay_seconds": 0.2,
            "payment_provider_retry_max_delay_seconds": 1.0,
            "payment_provider_retry_total_timeout_seconds": 10.0,
        }
    )
