"""End-to-end behaviour of POST /process-payment through the ASGI app."""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest
from fastapi import FastAPI

from app.config import Settings
from app.main import create_app
from tests.fakes import GatedProcessor, RecordingProcessor

pytestmark = pytest.mark.anyio

ENDPOINT = "/process-payment"
PAYMENT = {"amount": 100, "currency": "GHS"}


async def pay(
    client: httpx.AsyncClient, key: str | None, body: Any = PAYMENT, *, raw: str | None = None
) -> httpx.Response:
    headers = {} if key is None else {"Idempotency-Key": key}
    if raw is not None:
        headers["Content-Type"] = "application/json"
        return await client.post(ENDPOINT, content=raw, headers=headers)
    return await client.post(ENDPOINT, json=body, headers=headers)


def store_size(app: FastAPI) -> int:
    return len(app.state.idempotency_store)


def gateway_client(app: FastAPI, *, raise_app_exceptions: bool = True) -> httpx.AsyncClient:
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=raise_app_exceptions)
    return httpx.AsyncClient(transport=transport, base_url="http://gateway.test")


@pytest.fixture
def processor() -> RecordingProcessor:
    return RecordingProcessor()


@pytest.fixture
def app(processor: RecordingProcessor) -> FastAPI:
    return create_app(settings=Settings(), processor=processor)


@pytest.fixture
async def client(app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    async with gateway_client(app) as test_client:
        yield test_client


# --- Service entry points -------------------------------------------------------------


async def test_root_redirects_to_interactive_docs(client: httpx.AsyncClient) -> None:
    response = await client.get("/")

    assert response.status_code == 307
    assert response.headers["location"] == "/docs"


async def test_openapi_documents_the_payment_endpoint(client: httpx.AsyncClient) -> None:
    schema = (await client.get("/openapi.json")).json()

    operation = schema["paths"]["/process-payment"]["post"]
    assert {"200", "400", "409", "422", "503"} <= set(operation["responses"])
    assert any(p["name"] == "Idempotency-Key" for p in operation["parameters"])


# --- User story 1: first transaction --------------------------------------------------


async def test_first_request_charges_the_payment(
    client: httpx.AsyncClient, processor: RecordingProcessor
) -> None:
    response = await pay(client, "payment-001")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "Charged 100 GHS"
    assert body["transaction_id"].startswith("txn_")
    assert response.headers["X-Cache-Hit"] == "false"
    assert processor.calls == 1


async def test_decimal_amount_is_reported_in_status(client: httpx.AsyncClient) -> None:
    response = await pay(client, "payment-decimal", {"amount": 49.99, "currency": "USD"})

    assert response.status_code == 200
    assert response.json()["status"] == "Charged 49.99 USD"


async def test_different_keys_are_independent_payments(
    client: httpx.AsyncClient, processor: RecordingProcessor
) -> None:
    first = await pay(client, "order-1")
    second = await pay(client, "order-2")

    assert second.headers["X-Cache-Hit"] == "false"
    assert first.json()["transaction_id"] != second.json()["transaction_id"]
    assert processor.calls == 2


async def test_missing_idempotency_key_is_rejected(
    app: FastAPI, client: httpx.AsyncClient, processor: RecordingProcessor
) -> None:
    response = await pay(client, None)

    assert response.status_code == 400
    assert response.json() == {"detail": "Missing required Idempotency-Key header."}
    assert processor.calls == 0
    assert store_size(app) == 0


@pytest.mark.parametrize("key", ["", "   "])
async def test_blank_idempotency_key_is_rejected(
    app: FastAPI, client: httpx.AsyncClient, processor: RecordingProcessor, key: str
) -> None:
    response = await pay(client, key)

    assert response.status_code == 400
    assert "Idempotency-Key" in response.json()["detail"]
    assert processor.calls == 0
    assert store_size(app) == 0


@pytest.mark.parametrize("key", ["x" * 256, "contains space", "tab\tseparated"])
async def test_malformed_idempotency_key_is_rejected(
    app: FastAPI, client: httpx.AsyncClient, processor: RecordingProcessor, key: str
) -> None:
    response = await pay(client, key)

    assert response.status_code == 400
    assert "visible ASCII" in response.json()["detail"]
    assert processor.calls == 0
    assert store_size(app) == 0


async def test_longest_allowed_idempotency_key_is_accepted(client: httpx.AsyncClient) -> None:
    response = await pay(client, "k" * 255)

    assert response.status_code == 200

@pytest.mark.parametrize(
    "amount",
    [0, -5, "100", True, None, 100.123, 10**12],
    ids=["zero", "negative", "string", "boolean", "null", "three-decimals", "too-large"],
)
async def test_invalid_amount_is_rejected(
    app: FastAPI, client: httpx.AsyncClient, processor: RecordingProcessor, amount: Any
) -> None:
    response = await pay(client, "bad-amount", {"amount": amount, "currency": "GHS"})

    assert response.status_code == 422
    assert processor.calls == 0
    assert store_size(app) == 0


@pytest.mark.parametrize("currency", ["ghs", "GH", "GHSS", "G1S", 123, None])
async def test_invalid_currency_is_rejected(
    app: FastAPI, client: httpx.AsyncClient, processor: RecordingProcessor, currency: Any
) -> None:
    response = await pay(client, "bad-currency", {"amount": 100, "currency": currency})

    assert response.status_code == 422
    assert processor.calls == 0
    assert store_size(app) == 0


@pytest.mark.parametrize(
    "raw",
    [
        '{"amount": 100}',
        '{"currency": "GHS"}',
        '{"amount": 100, "currency": "GHS", "tip": 5}',
        "{not json",
        "",
    ],
    ids=["missing-currency", "missing-amount", "unknown-field", "malformed-json", "empty-body"],
)
async def test_invalid_body_is_rejected_without_creating_a_record(
    app: FastAPI, client: httpx.AsyncClient, processor: RecordingProcessor, raw: str
) -> None:
    response = await pay(client, "bad-body", raw=raw)

    assert response.status_code == 422
    assert processor.calls == 0
    assert store_size(app) == 0


async def test_invalid_retry_does_not_disturb_a_completed_record(
    client: httpx.AsyncClient, processor: RecordingProcessor
) -> None:
    original = await pay(client, "payment-keep")

    invalid = await pay(client, "payment-keep", {"amount": -1, "currency": "GHS"})
    replay = await pay(client, "payment-keep")

    assert invalid.status_code == 422
    assert replay.content == original.content
    assert replay.headers["X-Cache-Hit"] == "true"
    assert processor.calls == 1




async def test_duplicate_request_replays_the_stored_response(
    client: httpx.AsyncClient, processor: RecordingProcessor
) -> None:
    original = await pay(client, "payment-dup")
    duplicate = await pay(client, "payment-dup")

    assert duplicate.status_code == original.status_code
    assert duplicate.content == original.content
    assert duplicate.headers["content-type"] == original.headers["content-type"]
    assert duplicate.headers["X-Cache-Hit"] == "true"
    assert processor.calls == 1


@pytest.mark.parametrize(
    "raw",
    [
        '{"currency": "GHS", "amount": 100}',
        '{\n  "amount": 100,\n  "currency": "GHS"\n}',
        '{"amount": 100.0, "currency": "GHS"}',
        '{"amount": 100.00, "currency": "GHS"}',
    ],
    ids=["reordered-keys", "pretty-printed", "trailing-zero", "two-trailing-zeros"],
)
async def test_semantically_identical_body_is_a_duplicate(
    client: httpx.AsyncClient, processor: RecordingProcessor, raw: str
) -> None:
    original = await pay(client, "payment-canonical", raw='{"amount":100,"currency":"GHS"}')
    duplicate = await pay(client, "payment-canonical", raw=raw)

    assert duplicate.status_code == 200
    assert duplicate.content == original.content
    assert duplicate.headers["X-Cache-Hit"] == "true"
    assert processor.calls == 1




@pytest.mark.parametrize(
    "changed_body",
    [{"amount": 500, "currency": "GHS"}, {"amount": 100, "currency": "USD"}],
    ids=["different-amount", "different-currency"],
)
async def test_reused_key_with_different_body_is_rejected(
    client: httpx.AsyncClient, processor: RecordingProcessor, changed_body: dict[str, Any]
) -> None:
    await pay(client, "payment-reuse")

    conflict = await pay(client, "payment-reuse", changed_body)

    assert conflict.status_code == 409
    assert conflict.json() == {
        "detail": "Idempotency key already used for a different request body."
    }
    assert "X-Cache-Hit" not in conflict.headers
    assert processor.calls == 1


async def test_conflict_does_not_overwrite_the_original_result(
    client: httpx.AsyncClient, processor: RecordingProcessor
) -> None:
    original = await pay(client, "payment-guarded")
    await pay(client, "payment-guarded", {"amount": 999, "currency": "GHS"})

    replay = await pay(client, "payment-guarded")

    assert replay.content == original.content
    assert replay.json()["status"] == "Charged 100 GHS"
    assert processor.calls == 1



async def test_in_flight_duplicate_waits_and_receives_the_first_result() -> None:
    processor = GatedProcessor()
    app = create_app(settings=Settings(), processor=processor)
    async with gateway_client(app) as client:
        first = asyncio.create_task(pay(client, "payment-123"))
        await processor.started.wait()

        duplicate = asyncio.create_task(pay(client, "payment-123"))
        await asyncio.sleep(0.05)
        assert not duplicate.done(), "duplicate must block while the first request is processing"

        processor.release()
        first_response, duplicate_response = await asyncio.gather(first, duplicate)

    assert first_response.status_code == 200
    assert first_response.headers["X-Cache-Hit"] == "false"
    assert duplicate_response.status_code == 200
    assert duplicate_response.content == first_response.content
    assert duplicate_response.headers["X-Cache-Hit"] == "true"
    assert processor.calls == 1


async def test_burst_of_identical_requests_is_processed_exactly_once() -> None:
    processor = RecordingProcessor(delay_seconds=0.1)
    app = create_app(settings=Settings(), processor=processor)
    async with gateway_client(app) as client:
        responses = await asyncio.gather(*(pay(client, "payment-burst") for _ in range(25)))

    assert processor.calls == 1
    assert {r.status_code for r in responses} == {200}
    assert len({r.content for r in responses}) == 1
    assert [r.headers["X-Cache-Hit"] for r in responses].count("false") == 1


async def test_in_flight_request_with_different_body_conflicts_without_waiting() -> None:
    processor = GatedProcessor()
    app = create_app(settings=Settings(), processor=processor)
    async with gateway_client(app) as client:
        first = asyncio.create_task(pay(client, "payment-123"))
        await processor.started.wait()

        conflict = await asyncio.wait_for(
            pay(client, "payment-123", {"amount": 500, "currency": "GHS"}), timeout=1
        )
        assert not first.done()

        processor.release()
        first_response = await first

    assert conflict.status_code == 409
    assert conflict.json() == {
        "detail": "Idempotency key already used for a different request body."
    }
    assert first_response.status_code == 200
    assert first_response.json()["status"] == "Charged 100 GHS"
    assert processor.calls == 1


async def test_failed_in_flight_request_releases_waiters_and_the_key() -> None:
    processor = GatedProcessor(fail=True)
    app = create_app(settings=Settings(), processor=processor)
    async with gateway_client(app, raise_app_exceptions=False) as client:
        first = asyncio.create_task(pay(client, "payment-flaky"))
        await processor.started.wait()
        duplicate = asyncio.create_task(pay(client, "payment-flaky"))
        await asyncio.sleep(0.05)

        processor.release()
        first_response, duplicate_response = await asyncio.wait_for(
            asyncio.gather(first, duplicate), timeout=1
        )

        assert first_response.status_code == 500
        assert duplicate_response.status_code == 503
        assert "safe to retry" in duplicate_response.json()["detail"]
        assert store_size(app) == 0

        app.state.payment_processor = RecordingProcessor()
        retry = await pay(client, "payment-flaky")

    assert retry.status_code == 200
    assert retry.headers["X-Cache-Hit"] == "false"


async def test_default_configuration_simulates_two_second_processing() -> None:
    """Runs the real simulated processor with its default 2 s delay (no test double)."""
    app = create_app(settings=Settings())
    async with gateway_client(app) as client:
        started = time.perf_counter()
        first = asyncio.create_task(pay(client, "payment-default"))
        await asyncio.sleep(0.5)
        in_flight_duplicate = asyncio.create_task(pay(client, "payment-default"))
        first_response, duplicate_response = await asyncio.gather(first, in_flight_duplicate)
        concurrent_elapsed = time.perf_counter() - started

        replay_started = time.perf_counter()
        replay = await pay(client, "payment-default")
        replay_elapsed = time.perf_counter() - replay_started

    assert 1.9 <= concurrent_elapsed < 3.5, "one ~2 s charge, not two back-to-back"
    assert first_response.json()["status"] == "Charged 100 GHS"
    assert duplicate_response.content == first_response.content == replay.content
    assert duplicate_response.headers["X-Cache-Hit"] == "true"
    assert replay.headers["X-Cache-Hit"] == "true"
    assert replay_elapsed < 0.5

async def test_key_is_reusable_after_the_configured_ttl() -> None:
    ttl = 0.3
    processor = RecordingProcessor(delay_seconds=0)
    app = create_app(settings=Settings(idempotency_ttl_seconds=ttl), processor=processor)
    async with gateway_client(app) as client:
        original = await pay(client, "payment-ttl")
        replay = await pay(client, "payment-ttl")

        await asyncio.sleep(ttl + 0.1)
        after_expiry = await pay(client, "payment-ttl", {"amount": 250, "currency": "GHS"})

    assert replay.headers["X-Cache-Hit"] == "true"
    assert after_expiry.status_code == 200
    assert after_expiry.headers["X-Cache-Hit"] == "false"
    assert after_expiry.json()["status"] == "Charged 250 GHS"
    assert after_expiry.json()["transaction_id"] != original.json()["transaction_id"]
    assert processor.calls == 2
    assert store_size(app) == 1
