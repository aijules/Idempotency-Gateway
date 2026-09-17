"""Simulated payment processor used by the gateway."""

from __future__ import annotations

import asyncio
import uuid
from decimal import Decimal

from app.models import PaymentResponse


class SimulatedPaymentProcessor:
    """Stands in for a call to a card network or payment provider: waits, then approves."""

    def __init__(self, delay_seconds: float) -> None:
        self.delay_seconds = delay_seconds

    async def charge(self, amount: Decimal, currency: str) -> PaymentResponse:
        await asyncio.sleep(self.delay_seconds)
        return PaymentResponse(
            transaction_id=f"txn_{uuid.uuid4().hex}",
            status=f"Charged {amount} {currency}",
        )
