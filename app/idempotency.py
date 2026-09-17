"""Idempotency store: remembers which keys were used, for which request, and with what result."""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any


class IdempotencyKeyConflictError(Exception):
    """The key was already used with a different request body."""


class InFlightRequestFailedError(Exception):
    """This request waited on an identical in-flight request, and that request failed."""


@dataclass(frozen=True)
class StoredResponse:
    status_code: int
    body: bytes


@dataclass(frozen=True)
class IdempotentResult:
    response: StoredResponse
    replayed: bool


@dataclass(eq=False)
class IdempotencyRecord:
    """One Idempotency-Key and the request that claimed it.

    PROCESSING: ``finished`` is not set yet.
    COMPLETED:  ``finished`` is set and ``response`` holds the stored response.
    FAILED:     ``finished`` is set but ``response`` is None. A failed record is removed from
                the store right away; only requests already waiting on it ever see it.
    """

    key: str
    fingerprint: str
    response: StoredResponse | None = None
    finished: asyncio.Event = field(default_factory=asyncio.Event)


def fingerprint_payload(payload: dict[str, Any]) -> str:
    """SHA-256 of the payload as JSON with sorted keys and no insignificant whitespace.

    Two logically identical requests therefore always produce the same fingerprint.
    """
    canonical_json = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()


Operation = Callable[[], Awaitable[StoredResponse]]


class IdempotencyStore:
    """Process-local, in-memory registry of idempotency records.

    Every state change happens inside ``self._lock`` and contains no ``await``, so
    check-then-insert, completion, failure and expiry can never interleave. The lock is
    never held while an operation runs or while a duplicate waits for one to finish.

    Completed records are kept for ``ttl_seconds`` after completion and removed lazily at
    the start of the next request. ``clock`` must be monotonic.
    """

    def __init__(self, ttl_seconds: float, clock: Callable[[], float] = time.monotonic) -> None:
        self._ttl_seconds = ttl_seconds
        self._clock = clock
        self._records: dict[str, IdempotencyRecord] = {}
       
        self._expiry_queue: deque[tuple[float, IdempotencyRecord]] = deque()
        self._lock = asyncio.Lock()

    def __len__(self) -> int:
        return len(self._records)

    async def run_once(self, key: str, fingerprint: str, operation: Operation) -> IdempotentResult:
        """Run ``operation`` at most once per key and return its (possibly replayed) response.

        A duplicate that arrives while the first request is still processing waits for it
        and receives the same response.
        """
        async with self._lock:
            self._evict_expired()
            record = self._records.get(key)
            if record is None:
                record = IdempotencyRecord(key=key, fingerprint=fingerprint)
                self._records[key] = record
                is_first_request = True
            elif record.fingerprint != fingerprint:
                raise IdempotencyKeyConflictError(
                    "Idempotency key already used for a different request body."
                )
            else:
                is_first_request = False

        if is_first_request:
            response = await self._run_operation(record, operation)
            return IdempotentResult(response, replayed=False)

       
        await record.finished.wait()
        if record.response is None:
            raise InFlightRequestFailedError(
                "The in-flight request with this Idempotency-Key failed before completing. "
                "It is safe to retry with the same key."
            )
        return IdempotentResult(record.response, replayed=True)

    async def _run_operation(
        self, record: IdempotencyRecord, operation: Operation
    ) -> StoredResponse:
        try:
            response = await operation()
        except BaseException:
            # BaseException includes cancellation: wake the waiters and release the key
            # so that nobody hangs and the client can retry.
            async with self._lock:
                record.finished.set()
                self._remove(record)
            raise

        async with self._lock:
            record.response = response
            record.finished.set()
            self._expiry_queue.append((self._clock() + self._ttl_seconds, record))
        return response

    def _evict_expired(self) -> None:
        now = self._clock()
        while self._expiry_queue and self._expiry_queue[0][0] <= now:
            _, record = self._expiry_queue.popleft()
            self._remove(record)

    def _remove(self, record: IdempotencyRecord) -> None:
        # Only remove this exact record, never a newer record that reuses the same key.
        if self._records.get(record.key) is record:
            del self._records[record.key]
