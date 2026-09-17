"""Unit tests for the idempotency store: fingerprints, failure handling and record expiry.

Replay, conflict and concurrent-duplicate behaviour is covered end to end in test_payments.py.
"""

from __future__ import annotations

import asyncio
import hashlib
from decimal import Decimal

import pytest

from app.idempotency import (
    IdempotencyKeyConflictError,
    IdempotencyStore,
    InFlightRequestFailedError,
    StoredResponse,
    fingerprint_payload,
)
from app.models import PaymentRequest

pytestmark = pytest.mark.anyio

OK = StoredResponse(status_code=200, body=b'{"status":"Charged 100 GHS"}')
TTL = 60.0


class CountingOperation:
    def __init__(self) -> None:
        self.calls = 0

    async def __call__(self) -> StoredResponse:
        self.calls += 1
        return OK


class GatedOperation(CountingOperation):
    """Stays in flight until ``release()``; optionally fails afterwards."""

    def __init__(self, *, fail: bool = False) -> None:
        super().__init__()
        self.started = asyncio.Event()
        self.gate = asyncio.Event()
        self.fail = fail

    def release(self) -> None:
        self.gate.set()

    async def __call__(self) -> StoredResponse:
        self.started.set()
        await self.gate.wait()
        if self.fail:
            raise RuntimeError("processor unavailable")
        return await super().__call__()


class FakeClock:
    def __init__(self) -> None:
        self.now = 1_000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


async def let_other_tasks_run() -> None:
    """Give every runnable task a chance to reach its next await point."""
    for _ in range(5):
        await asyncio.sleep(0)


# --- Fingerprinting -------------------------------------------------------------------


def test_fingerprint_is_sha256_of_compact_sorted_json() -> None:
    fingerprint = fingerprint_payload({"currency": "GHS", "amount": "100"})

    assert fingerprint == hashlib.sha256(b'{"amount":"100","currency":"GHS"}').hexdigest()


@pytest.mark.parametrize(
    "other", [{"amount": "500", "currency": "GHS"}, {"amount": "100", "currency": "USD"}]
)
def test_fingerprint_changes_when_the_payload_changes(other: dict[str, str]) -> None:
    assert fingerprint_payload({"amount": "100", "currency": "GHS"}) != fingerprint_payload(other)


@pytest.mark.parametrize("amount", [100, 100.0, Decimal("100.00"), Decimal("1E+2")])
def test_equal_amounts_produce_the_same_payload(amount: object) -> None:
    request = PaymentRequest.model_validate({"amount": amount, "currency": "GHS"})

    assert request.model_dump(mode="json") == {"amount": "100", "currency": "GHS"}


async def test_owner_failure_wakes_waiters_and_releases_the_key() -> None:
    store = IdempotencyStore(ttl_seconds=TTL)
    operation = GatedOperation(fail=True)
    owner = asyncio.create_task(store.run_once("key", "fp", operation))
    await operation.started.wait()
    waiters = [asyncio.create_task(store.run_once("key", "fp", operation)) for _ in range(3)]
    await let_other_tasks_run()

    operation.release()
    outcomes = await asyncio.wait_for(
        asyncio.gather(owner, *waiters, return_exceptions=True), timeout=1
    )

    assert isinstance(outcomes[0], RuntimeError)
    assert all(isinstance(outcome, InFlightRequestFailedError) for outcome in outcomes[1:])
    assert len(store) == 0
    assert (await store.run_once("key", "fp", CountingOperation())).replayed is False


async def test_owner_cancellation_wakes_waiters_and_releases_the_key() -> None:
    store = IdempotencyStore(ttl_seconds=TTL)
    operation = GatedOperation()
    owner = asyncio.create_task(store.run_once("key", "fp", operation))
    await operation.started.wait()
    waiter = asyncio.create_task(store.run_once("key", "fp", operation))
    await let_other_tasks_run()

    owner.cancel()

    with pytest.raises(InFlightRequestFailedError):
        await asyncio.wait_for(waiter, timeout=1)
    assert owner.cancelled()
    assert len(store) == 0


async def test_cancelled_waiter_does_not_affect_the_owner() -> None:
    store = IdempotencyStore(ttl_seconds=TTL)
    operation = GatedOperation()
    owner = asyncio.create_task(store.run_once("key", "fp", operation))
    await operation.started.wait()
    waiter = asyncio.create_task(store.run_once("key", "fp", operation))
    await let_other_tasks_run()

    waiter.cancel()
    await let_other_tasks_run()
    operation.release()

    assert (await owner).replayed is False
    assert (await store.run_once("key", "fp", operation)).replayed is True
    assert operation.calls == 1


async def test_unrelated_keys_are_not_blocked_by_an_in_flight_operation() -> None:
    store = IdempotencyStore(ttl_seconds=TTL)
    slow = GatedOperation()
    owner = asyncio.create_task(store.run_once("slow-key", "fp", slow))
    await slow.started.wait()

    other = await asyncio.wait_for(
        store.run_once("other-key", "fp", CountingOperation()), timeout=1
    )

    assert other.replayed is False
    assert not owner.done()
    slow.release()
    await owner

async def test_completed_record_is_replayed_until_the_ttl_elapses() -> None:
    clock = FakeClock()
    store = IdempotencyStore(ttl_seconds=10, clock=clock)
    operation = CountingOperation()
    await store.run_once("key", "fp", operation)

    clock.advance(9.999)

    assert (await store.run_once("key", "fp", operation)).replayed is True
    assert operation.calls == 1


async def test_expired_key_is_processed_as_a_new_request() -> None:
    clock = FakeClock()
    store = IdempotencyStore(ttl_seconds=10, clock=clock)
    operation = CountingOperation()
    await store.run_once("key", "fp", operation)

    clock.advance(10)

    assert (await store.run_once("key", "fp", operation)).replayed is False
    assert operation.calls == 2


async def test_expired_key_can_be_reused_for_a_different_body() -> None:
    clock = FakeClock()
    store = IdempotencyStore(ttl_seconds=10, clock=clock)
    await store.run_once("key", "fp-1", CountingOperation())

    clock.advance(10)

    assert (await store.run_once("key", "fp-2", CountingOperation())).replayed is False
    with pytest.raises(IdempotencyKeyConflictError):
        await store.run_once("key", "fp-1", CountingOperation())


async def test_expired_records_are_evicted_even_if_their_keys_are_never_reused() -> None:
    clock = FakeClock()
    store = IdempotencyStore(ttl_seconds=10, clock=clock)
    for key in ("a", "b", "c"):
        await store.run_once(key, "fp", CountingOperation())
        clock.advance(1)
    assert len(store) == 3

    clock.advance(8.5)  # a and b have expired, c has 0.5 s left
    await store.run_once("d", "fp", CountingOperation())

    assert len(store) == 2
    assert (await store.run_once("c", "fp", CountingOperation())).replayed is True


async def test_processing_record_never_expires() -> None:
    clock = FakeClock()
    store = IdempotencyStore(ttl_seconds=10, clock=clock)
    operation = GatedOperation()
    owner = asyncio.create_task(store.run_once("key", "fp", operation))
    await operation.started.wait()

    clock.advance(1_000)
    waiter = asyncio.create_task(store.run_once("key", "fp", operation))
    await let_other_tasks_run()
    assert not waiter.done()

    operation.release()
    owner_result, waiter_result = await asyncio.gather(owner, waiter)
    assert waiter_result.response == owner_result.response
    assert operation.calls == 1


async def test_ttl_is_measured_from_completion_not_arrival() -> None:
    clock = FakeClock()
    store = IdempotencyStore(ttl_seconds=10, clock=clock)
    operation = GatedOperation()
    owner = asyncio.create_task(store.run_once("key", "fp", operation))
    await operation.started.wait()

    clock.advance(50)  
    operation.release()
    await owner

    clock.advance(9)
    assert (await store.run_once("key", "fp", operation)).replayed is True
    clock.advance(1)
    assert (await store.run_once("key", "fp", operation)).replayed is False
