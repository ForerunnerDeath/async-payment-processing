from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.core.config import Settings
from app.main import (
    SERVICE_NAME,
    SERVICE_VERSION,
    app,
    lifespan,
)


async def test_lifespan_initializes_and_closes_resources(
    settings: Settings,
) -> None:
    database = MagicMock()
    database.close = AsyncMock()

    logger = MagicMock()

    with (
        patch(
            "app.main.get_settings",
            return_value=settings,
        ) as get_settings,
        patch("app.main.configure_logging") as configure_logging,
        patch(
            "app.main.Database",
            return_value=database,
        ) as database_class,
        patch(
            "app.main.structlog.get_logger",
            return_value=logger,
        ),
    ):
        async with lifespan(app):
            assert app.state.settings is settings
            assert app.state.database is database

            get_settings.assert_called_once_with()
            configure_logging.assert_called_once_with(settings.log_level)
            database_class.assert_called_once_with(settings)

            database.close.assert_not_awaited()

            logger.info.assert_any_call(
                "service_started",
                service=SERVICE_NAME,
                version=SERVICE_VERSION,
                environment=settings.environment,
            )

    database.close.assert_awaited_once_with()

    logger.info.assert_any_call(
        "service_stopped",
        service=SERVICE_NAME,
        version=SERVICE_VERSION,
    )


async def test_lifespan_closes_database_when_application_fails(
    settings: Settings,
) -> None:
    database = MagicMock()
    database.close = AsyncMock()

    with (
        patch(
            "app.main.get_settings",
            return_value=settings,
        ),
        patch("app.main.configure_logging"),
        patch(
            "app.main.Database",
            return_value=database,
        ),
        patch("app.main.structlog.get_logger"),
        pytest.raises(RuntimeError, match="application failed"),
    ):
        async with lifespan(app):
            raise RuntimeError("application failed")

    database.close.assert_awaited_once_with()


def test_fastapi_application_metadata() -> None:
    assert app.title == "Async Payment Processing"
    assert app.version == SERVICE_VERSION
