"""Application factory and the ASGI app served by uvicorn (``app.main:app``)."""

from __future__ import annotations

from fastapi import FastAPI

from app.api import router
from app.config import Settings
from app.idempotency import IdempotencyStore
from app.payments import SimulatedPaymentProcessor


def create_app(
    settings: Settings | None = None, processor: SimulatedPaymentProcessor | None = None
) -> FastAPI:
    """Build the gateway. Tests pass their own settings and processor."""
    if settings is None:
        settings = Settings.from_env()
    if processor is None:
        processor = SimulatedPaymentProcessor(settings.processing_delay_seconds)

    app = FastAPI(
        title="Idempotency Gateway",
        version="1.0.0",
        description="Payment API that processes each Idempotency-Key exactly once.",
    )
    app.state.idempotency_store = IdempotencyStore(ttl_seconds=settings.idempotency_ttl_seconds)
    app.state.payment_processor = processor
    app.include_router(router)
    return app


app = create_app()
