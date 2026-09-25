from fastapi import APIRouter

from app.api.health import router as health_router
from app.api.payments import router as payments_router

router = APIRouter()
router.include_router(payments_router, prefix="/api/v1")
router.include_router(health_router)
