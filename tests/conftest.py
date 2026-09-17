import pytest


@pytest.fixture
def anyio_backend() -> str:
    # The store relies on asyncio primitives, so only run async tests on asyncio.
    return "asyncio"
