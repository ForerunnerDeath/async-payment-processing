from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI

from app.api.router import router
from app.core.config import get_settings
from app.core.database import Database
from app.core.logging import configure_logging
from app.core.middleware import request_id_middleware

SERVICE_NAME = "async-payment-processing"
SERVICE_VERSION = "0.1.0"


@asynccontextmanager
async def lifespan(application: FastAPI) -> AsyncGenerator[None]:
    settings = get_settings()

    configure_logging(settings.log_level)

    logger = structlog.get_logger()
    database = Database(settings)

    application.state.settings = settings
    application.state.database = database

    logger.info(
        "service_started",
        service=SERVICE_NAME,
        version=SERVICE_VERSION,
        environment=settings.environment,
    )

    try:
        yield
    finally:
        await database.close()

        logger.info(
            "service_stopped",
            service=SERVICE_NAME,
            version=SERVICE_VERSION,
        )


app = FastAPI(
    title="Async Payment Processing",
    version=SERVICE_VERSION,
    lifespan=lifespan,
)

app.middleware("http")(request_id_middleware)
app.include_router(router)
