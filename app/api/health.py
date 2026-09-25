from typing import Annotated

from fastapi import APIRouter, Depends, Response, status
from sqlalchemy.exc import SQLAlchemyError

from app.api.dependencies import get_database, verify_api_key
from app.core.database import Database

router = APIRouter(
    prefix="/health",
    tags=["health"],
    dependencies=[Depends(verify_api_key)],
)


@router.get("/live")
async def live() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/ready")
async def ready(
    response: Response,
    database: Annotated[Database, Depends(get_database)],
) -> dict[str, str]:
    checks = {"postgres": "ok"}

    try:
        await database.check_connection()
    except SQLAlchemyError, RuntimeError, OSError:
        checks["postgres"] = "unavailable"
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE

    return checks
