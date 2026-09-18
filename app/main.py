"""Application factory and the ASGI app served by uvicorn (``app.main:app``)."""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError

from app.api import router, unhandled_exception_handler, validation_exception_handler
from app.config import Settings
from app.idempotency import IdempotencyStore
from app.logging_config import configure_logging
from app.middleware import MaxBodySizeMiddleware
from app.payments import SimulatedPaymentProcessor


def create_app(
    settings: Settings | None = None, processor: SimulatedPaymentProcessor | None = None
) -> FastAPI:
    """Build the gateway. Tests pass their own settings and processor."""
    if settings is None:
        settings = Settings.from_env()
    if processor is None:
        processor = SimulatedPaymentProcessor(settings.processing_delay_seconds)

    configure_logging(settings.log_level)

    app = FastAPI(
        title="Idempotency Gateway",
        version="1.0.0",
        description="Payment API that processes each Idempotency-Key exactly once.",
    )
    app.state.idempotency_store = IdempotencyStore(ttl_seconds=settings.idempotency_ttl_seconds)
    app.state.payment_processor = processor
    app.add_middleware(MaxBodySizeMiddleware, max_bytes=settings.max_request_body_bytes)
    app.add_exception_handler(RequestValidationError, validation_exception_handler)
    app.add_exception_handler(Exception, unhandled_exception_handler)
    app.include_router(router)
    return app


app = create_app()
