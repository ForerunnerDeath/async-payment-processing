from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.dependencies import get_session, verify_api_key
from app.schemas.payment import PaymentAccepted, PaymentCreate, PaymentDetail
from app.services.payment import IdempotencyConflictError, PaymentService
from app.services.payment_query import PaymentQueryService

router = APIRouter(
    prefix="/payments",
    tags=["payments"],
    dependencies=[Depends(verify_api_key)],
)


@router.post("", response_model=PaymentAccepted, status_code=status.HTTP_202_ACCEPTED)
async def create_payment(
    data: PaymentCreate,
    idempotency_key: Annotated[
        str,
        Header(
            alias="Idempotency-Key",
            min_length=1,
            max_length=255,
        ),
    ],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> PaymentAccepted:
    normalized_idempotency_key = idempotency_key.strip()

    if not normalized_idempotency_key:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="Idempotency-Key must not be blank",
        )

    service = PaymentService(session)

    try:
        result = await service.create_payment(
            data,
            idempotency_key=normalized_idempotency_key,
        )
    except IdempotencyConflictError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(exc),
        ) from exc

    return PaymentAccepted(
        payment_id=result.payment.id,
        status=result.payment.status,
        created_at=result.payment.created_at,
    )


@router.get("/{payment_id}", response_model=PaymentDetail)
async def get_payment(
    payment_id: UUID,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> PaymentDetail:
    service = PaymentQueryService(session)

    payment = await service.get_payment(payment_id)

    if payment is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Payment not found",
        )

    return PaymentDetail.model_validate(payment)
