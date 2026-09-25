from secrets import compare_digest
from typing import Annotated, cast

from fastapi import Header, HTTPException, Request, status

from app.core.config import Settings
from app.core.database import Database


def get_settings(request: Request) -> Settings:
    return cast(Settings, request.app.state.settings)


def get_database(request: Request) -> Database:
    return cast(Database, request.app.state.database)


def verify_api_key(
    request: Request,
    x_api_key: Annotated[str | None, Header(alias="X-API-Key")] = None,
) -> None:
    settings = get_settings(request)
    expected_api_key = settings.api_key.get_secret_value()

    if x_api_key is None or not compare_digest(x_api_key, expected_api_key):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
        )
