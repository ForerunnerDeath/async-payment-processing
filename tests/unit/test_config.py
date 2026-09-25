import pytest
from pydantic import ValidationError

from app.core.config import Environment, Settings


def make_settings(**overrides: object) -> Settings:
    data: dict[str, object] = {
        "api_key": "test-api-key",
        "database_url": ("postgresql+asyncpg://payments:payments@localhost:5432/payments"),
    }
    data.update(overrides)

    return Settings.model_validate(data)


def test_settings_have_expected_defaults() -> None:
    settings = make_settings()

    assert settings.app_name == "Async Payment Processing"
    assert settings.environment is Environment.LOCAL
    assert settings.log_level == "INFO"


def test_api_key_is_hidden_from_settings_repr() -> None:
    secret = "super-secret-api-key"

    settings = make_settings(api_key=secret)

    assert secret not in repr(settings)
    assert settings.api_key.get_secret_value() == secret


def test_invalid_database_url_is_rejected() -> None:
    with pytest.raises(ValidationError):
        make_settings(database_url="not-a-postgres-url")
