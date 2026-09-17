from __future__ import annotations

import pytest

from app.config import Settings


def test_defaults_apply_when_environment_is_empty() -> None:
    settings = Settings.from_env({})

    assert settings.idempotency_ttl_seconds == 86_400
    assert settings.processing_delay_seconds == 2.0


def test_values_are_read_from_the_environment() -> None:
    settings = Settings.from_env(
        {"IDEMPOTENCY_TTL_SECONDS": "3600", "PAYMENT_PROCESSING_DELAY_SECONDS": "0.5"}
    )

    assert settings.idempotency_ttl_seconds == 3600
    assert settings.processing_delay_seconds == 0.5


def test_blank_values_fall_back_to_defaults() -> None:
    settings = Settings.from_env(
        {"IDEMPOTENCY_TTL_SECONDS": " ", "PAYMENT_PROCESSING_DELAY_SECONDS": ""}
    )

    assert settings == Settings()


@pytest.mark.parametrize("raw", ["abc", "0", "-60", "nan", "inf"])
def test_invalid_ttl_is_rejected(raw: str) -> None:
    with pytest.raises(ValueError, match=r"(?i)ttl"):
        Settings.from_env({"IDEMPOTENCY_TTL_SECONDS": raw})


@pytest.mark.parametrize("raw", ["abc", "-1", "nan", "inf"])
def test_invalid_delay_is_rejected(raw: str) -> None:
    with pytest.raises(ValueError, match=r"(?i)delay"):
        Settings.from_env({"PAYMENT_PROCESSING_DELAY_SECONDS": raw})


def test_zero_delay_is_allowed() -> None:
    assert Settings(processing_delay_seconds=0).processing_delay_seconds == 0
