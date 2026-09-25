import pytest

from app.core.config import Settings


@pytest.fixture
def settings() -> Settings:
    return Settings.model_validate(
        {
            "api_key": "test-api-key",
            "database_url": ("postgresql+asyncpg://payments:payments@localhost:5432/payments"),
        }
    )
