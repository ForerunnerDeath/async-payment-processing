from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from app.core import database as database_module
from app.core.config import Settings


def test_database_creates_its_own_engine_and_session_factory(
    settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = MagicMock(spec=AsyncEngine)
    session_factory = MagicMock()

    create_async_engine = MagicMock(return_value=engine)
    create_session_factory = MagicMock(return_value=session_factory)

    monkeypatch.setattr(
        database_module,
        "create_async_engine",
        create_async_engine,
    )
    monkeypatch.setattr(
        database_module,
        "async_sessionmaker",
        create_session_factory,
    )

    database = database_module.Database(settings)

    create_async_engine.assert_called_once_with(
        str(settings.database_url),
        pool_pre_ping=True,
    )
    create_session_factory.assert_called_once_with(
        bind=engine,
        expire_on_commit=False,
    )

    assert database.session_factory is session_factory


async def test_database_checks_connection(
    settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = MagicMock(spec=AsyncEngine)
    session_factory = MagicMock()

    connection = AsyncMock()

    connection_context = MagicMock()
    connection_context.__aenter__ = AsyncMock(return_value=connection)
    connection_context.__aexit__ = AsyncMock(return_value=None)

    engine.connect.return_value = connection_context

    monkeypatch.setattr(
        database_module,
        "create_async_engine",
        MagicMock(return_value=engine),
    )
    monkeypatch.setattr(
        database_module,
        "async_sessionmaker",
        MagicMock(return_value=session_factory),
    )

    database = database_module.Database(settings)

    await database.check_connection()

    engine.connect.assert_called_once_with()
    connection.execute.assert_awaited_once()


async def test_database_closes_engine(
    settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = MagicMock(spec=AsyncEngine)
    engine.dispose = AsyncMock()

    session_factory = MagicMock()

    monkeypatch.setattr(
        database_module,
        "create_async_engine",
        MagicMock(return_value=engine),
    )
    monkeypatch.setattr(
        database_module,
        "async_sessionmaker",
        MagicMock(return_value=session_factory),
    )

    database = database_module.Database(settings)

    await database.close()

    engine.dispose.assert_awaited_once_with()
