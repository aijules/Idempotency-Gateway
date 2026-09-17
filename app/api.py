"""HTTP layer: validates requests and turns idempotency store results into responses."""

from __future__ import annotations

import re
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Request, Response, status
from fastapi.responses import JSONResponse, RedirectResponse

from app.idempotency import (
    IdempotencyKeyConflictError,
    IdempotencyStore,
    InFlightRequestFailedError,
    StoredResponse,
    fingerprint_payload,
)
from app.models import ErrorResponse, PaymentRequest, PaymentResponse
from app.payments import SimulatedPaymentProcessor


VALID_IDEMPOTENCY_KEY = re.compile(r"[\x21-\x7e]{1,255}")

router = APIRouter()


def require_idempotency_key(
    idempotency_key: Annotated[
        str | None,
        Header(
            alias="Idempotency-Key",
            description=(
                "Required. Unique per logical payment, 1-255 visible ASCII characters. "
                "Reuse it when retrying the same payment."
            ),
        ),
    ] = None,
) -> str:
   
    if idempotency_key is None or not idempotency_key.strip():
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Missing required Idempotency-Key header.")
    if not VALID_IDEMPOTENCY_KEY.fullmatch(idempotency_key):
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            "Idempotency-Key must be 1-255 visible ASCII characters without whitespace.",
        )
    return idempotency_key


@router.get("/", include_in_schema=False)
async def root() -> RedirectResponse:
    return RedirectResponse(url="/docs")


@router.post(
    "/process-payment",
    response_model=PaymentResponse,
    summary="Charge a payment exactly once per Idempotency-Key",
    responses={
        status.HTTP_400_BAD_REQUEST: {
            "model": ErrorResponse,
            "description": "Missing or malformed Idempotency-Key.",
        },
        status.HTTP_409_CONFLICT: {
            "model": ErrorResponse,
            "description": "Key reused with a different body.",
        },
        status.HTTP_503_SERVICE_UNAVAILABLE: {
            "model": ErrorResponse,
            "description": "The identical in-flight request this one waited on failed.",
        },
    },
)
async def process_payment(
    payment: PaymentRequest,
    idempotency_key: Annotated[str, Depends(require_idempotency_key)],
    request: Request,
) -> Response:
    store: IdempotencyStore = request.app.state.idempotency_store
    processor: SimulatedPaymentProcessor = request.app.state.payment_processor

    async def charge_payment() -> StoredResponse:
        payment_response = await processor.charge(payment.amount, payment.currency)
        return StoredResponse(status.HTTP_200_OK, payment_response.model_dump_json().encode())

    fingerprint = fingerprint_payload(payment.model_dump(mode="json"))
    try:
        result = await store.run_once(idempotency_key, fingerprint, charge_payment)
    except IdempotencyKeyConflictError as error:
        return JSONResponse({"detail": str(error)}, status_code=status.HTTP_409_CONFLICT)
    except InFlightRequestFailedError as error:
        return JSONResponse({"detail": str(error)}, status_code=status.HTTP_503_SERVICE_UNAVAILABLE)

    return Response(
        content=result.response.body,
        status_code=result.response.status_code,
        media_type="application/json",
        headers={"X-Cache-Hit": "true" if result.replayed else "false"},
    )

