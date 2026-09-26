import pytest
from pydantic import ValidationError
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
)

from app.core.config import Environment, Settings


class IsolatedSettings(Settings):
    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (init_settings,)


def make_settings(**overrides: object) -> Settings:
    data: dict[str, object] = {
        "api_key": "test-api-key",
        "database_url": ("postgresql+asyncpg://payments:payments@localhost:5432/payments"),
        "rabbitmq_url": "amqp://payments:payments@localhost:5672/",
        "payment_provider_url": "http://localhost:8001",
    }
    data.update(overrides)

    return IsolatedSettings.model_validate(data)


def test_settings_have_expected_defaults() -> None:
    settings = make_settings()

    assert settings.app_name == "Async Payment Processing"
    assert settings.environment is Environment.LOCAL
    assert settings.log_level == "INFO"
    assert settings.outbox_relay_batch_size == 100
    assert settings.outbox_relay_poll_interval_seconds == 1.0
    assert settings.rabbit_publish_timeout_seconds == 5.0


def test_api_key_is_hidden_from_settings_repr() -> None:
    secret = "super-secret-api-key"

    settings = make_settings(api_key=secret)

    assert secret not in repr(settings)
    assert settings.api_key.get_secret_value() == secret


def test_invalid_database_url_is_rejected() -> None:
    with pytest.raises(ValidationError):
        make_settings(database_url="not-a-postgres-url")


def test_settings_helper_ignores_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "OUTBOX_RELAY_BATCH_SIZE",
        "777",
    )
    monkeypatch.setenv(
        "LOG_LEVEL",
        "ERROR",
    )
    monkeypatch.setenv(
        "PAYMENT_PROVIDER_URL",
        "https://ambient-environment.example",
    )

    settings = make_settings()

    assert settings.outbox_relay_batch_size == 100
    assert settings.log_level == "INFO"
    assert str(settings.payment_provider_url) == "http://localhost:8001/"
