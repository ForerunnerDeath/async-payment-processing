from collections.abc import AsyncGenerator

import pytest
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    create_async_engine,
)

from app.core.config import get_settings


@pytest.fixture
async def db_session() -> AsyncGenerator[AsyncSession]:
    settings = get_settings()

    engine = create_async_engine(
        str(settings.database_url),
        pool_pre_ping=True,
    )

    try:
        async with engine.connect() as connection:
            transaction = await connection.begin()

            session = AsyncSession(
                bind=connection,
                expire_on_commit=False,
                join_transaction_mode="create_savepoint",
            )

            try:
                yield session
            finally:
                await session.close()

                if transaction.is_active:
                    await transaction.rollback()
    finally:
        await engine.dispose()
