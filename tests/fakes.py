"""Test doubles for the payment processor."""

from __future__ import annotations

import asyncio
from decimal import Decimal

from app.models import PaymentResponse
from app.payments import SimulatedPaymentProcessor


class RecordingProcessor(SimulatedPaymentProcessor):
    """Simulated processor that counts how many charges were actually executed."""

    def __init__(self, delay_seconds: float = 0.05) -> None:
        super().__init__(delay_seconds)
        self.calls = 0

    async def charge(self, amount: Decimal, currency: str) -> PaymentResponse:
        self.calls += 1
        return await super().charge(amount, currency)


class GatedProcessor(RecordingProcessor):
    """Holds every charge in flight until the test calls ``release()``.

    Lets concurrency tests observe a request that is deterministically still processing,
    instead of racing against a timer.
    """

    def __init__(self, *, fail: bool = False) -> None:
        super().__init__(delay_seconds=0)
        self.started = asyncio.Event()
        self.gate = asyncio.Event()
        self.fail = fail

    def release(self) -> None:
        self.gate.set()

    async def charge(self, amount: Decimal, currency: str) -> PaymentResponse:
        self.started.set()
        await self.gate.wait()
        if self.fail:
            raise RuntimeError("payment processor unavailable")
        return await super().charge(amount, currency)
