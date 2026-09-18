"""HTTP layer: validates requests and turns idempotency store results into responses."""

from __future__ import annotations

import math
import re
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Request, Response, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, RedirectResponse

from app.idempotency import (
    IdempotencyKeyConflictError,
    IdempotencyStore,
    InFlightRequestFailedError,
    StoredResponse,
    fingerprint_payload,
)
from app.logging_config import get_logger
from app.models import ErrorResponse, HealthResponse, PaymentRequest, PaymentResponse
from app.payments import SimulatedPaymentProcessor


VALID_IDEMPOTENCY_KEY = re.compile(r"[\x21-\x7e]{1,255}")

router = APIRouter()
logger = get_logger()


def _json_safe_float(value: float) -> float | str:
    """Starlette's JSONResponse serializes with allow_nan=False. A rejected NaN/Infinity
    amount is echoed back inside a validation error as a raw non-finite float, which
    would otherwise crash that encoding step and turn a clean 422 into a 500."""
    return value if math.isfinite(value) else str(value)


async def validation_exception_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    errors = jsonable_encoder(exc.errors(), custom_encoder={float: _json_safe_float})
    return JSONResponse({"detail": errors}, status_code=status.HTTP_422_UNPROCESSABLE_CONTENT)


async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Log the failure server-side and return a generic body: never a stack trace."""
    logger.error("unhandled_exception path=%s error=%r", request.url.path, exc, exc_info=exc)
    return JSONResponse(
        {"detail": "Internal Server Error"}, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
    )


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


@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Liveness probe",
    description="Always returns 200 while the process is up. Used by Railway/uptime checks.",
)
async def health() -> HealthResponse:
    return HealthResponse(status="ok")


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
    short_fingerprint = fingerprint[:12]
    try:
        result = await store.run_once(idempotency_key, fingerprint, charge_payment)
    except IdempotencyKeyConflictError as error:
        logger.warning(
            "idempotency_key=%s fingerprint=%s outcome=conflict", idempotency_key, short_fingerprint
        )
        return JSONResponse({"detail": str(error)}, status_code=status.HTTP_409_CONFLICT)
    except InFlightRequestFailedError as error:
        logger.warning(
            "idempotency_key=%s fingerprint=%s outcome=in_flight_failed",
            idempotency_key,
            short_fingerprint,
        )
        return JSONResponse({"detail": str(error)}, status_code=status.HTTP_503_SERVICE_UNAVAILABLE)

    logger.info(
        "idempotency_key=%s fingerprint=%s outcome=%s status=%d",
        idempotency_key,
        short_fingerprint,
        "replayed" if result.replayed else "charged",
        result.response.status_code,
    )
    return Response(
        content=result.response.body,
        status_code=result.response.status_code,
        media_type="application/json",
        headers={"X-Cache-Hit": "true" if result.replayed else "false"},
    )

