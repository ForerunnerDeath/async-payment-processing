from unittest.mock import AsyncMock, MagicMock

import httpx
from fastapi import FastAPI

from app.api.health import router
from app.core.config import Settings


def create_test_app(
    settings: Settings,
    database: MagicMock,
) -> FastAPI:
    application = FastAPI()

    application.state.settings = settings
    application.state.database = database

    application.include_router(router)

    return application


def create_test_client(application: FastAPI) -> httpx.AsyncClient:
    transport = httpx.ASGITransport(app=application)

    return httpx.AsyncClient(
        transport=transport,
        base_url="http://testserver",
    )


async def test_live_requires_api_key(
    settings: Settings,
) -> None:
    database = MagicMock()
    application = create_test_app(settings, database)

    async with create_test_client(application) as client:
        response = await client.get("/health/live")

    assert response.status_code == 401
    assert response.json() == {
        "detail": "Invalid or missing API key",
    }


async def test_live_rejects_invalid_api_key(
    settings: Settings,
) -> None:
    database = MagicMock()
    application = create_test_app(settings, database)

    async with create_test_client(application) as client:
        response = await client.get(
            "/health/live",
            headers={"X-API-Key": "wrong-api-key"},
        )

    assert response.status_code == 401


async def test_live_returns_ok(
    settings: Settings,
) -> None:
    database = MagicMock()
    application = create_test_app(settings, database)

    async with create_test_client(application) as client:
        response = await client.get(
            "/health/live",
            headers={
                "X-API-Key": settings.api_key.get_secret_value(),
            },
        )

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


async def test_ready_returns_ok_when_database_is_available(
    settings: Settings,
) -> None:
    database = MagicMock()
    database.check_connection = AsyncMock()

    application = create_test_app(settings, database)

    async with create_test_client(application) as client:
        response = await client.get(
            "/health/ready",
            headers={
                "X-API-Key": settings.api_key.get_secret_value(),
            },
        )

    assert response.status_code == 200
    assert response.json() == {"postgres": "ok"}

    database.check_connection.assert_awaited_once_with()


async def test_ready_returns_service_unavailable_when_database_is_unavailable(
    settings: Settings,
) -> None:
    database = MagicMock()
    database.check_connection = AsyncMock(
        side_effect=OSError("database unavailable"),
    )

    application = create_test_app(settings, database)

    async with create_test_client(application) as client:
        response = await client.get(
            "/health/ready",
            headers={
                "X-API-Key": settings.api_key.get_secret_value(),
            },
        )

    assert response.status_code == 503
    assert response.json() == {
        "postgres": "unavailable",
    }

    database.check_connection.assert_awaited_once_with()
