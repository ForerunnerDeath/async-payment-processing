from enum import StrEnum
from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import AmqpDsn, Field, PostgresDsn, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_DIR = Path(__file__).resolve().parents[2]


class Environment(StrEnum):
    LOCAL = "local"
    TEST = "test"
    PRODUCTION = "production"


class Settings(BaseSettings):
    app_name: str = "Async Payment Processing"
    environment: Environment = Environment.LOCAL
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"

    api_key: SecretStr
    database_url: PostgresDsn

    rabbitmq_url: AmqpDsn

    outbox_relay_batch_size: int = Field(default=100, ge=1, le=1000)
    outbox_relay_poll_interval_seconds: float = Field(default=1.0, gt=0)
    rabbit_publish_timeout_seconds: float = Field(default=5.0, gt=0)

    model_config = SettingsConfigDict(
        env_file=PROJECT_DIR / ".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
        frozen=True,
    )


@lru_cache
def get_settings() -> Settings:
    # BaseSettings loads required values from environment/.env at runtime.
    # Pyright cannot represent that behavior in the generated constructor signature.
    return Settings()  # pyright: ignore[reportCallIssue]
