"""Runtime configuration loaded from environment variables."""

from __future__ import annotations

import math
import os
from collections.abc import Mapping
from dataclasses import dataclass

DEFAULT_IDEMPOTENCY_TTL_SECONDS = 86_400.0  # 24 hours
DEFAULT_PROCESSING_DELAY_SECONDS = 2.0
DEFAULT_MAX_REQUEST_BODY_BYTES = 16_384  # generous headroom for a two-field JSON payload
DEFAULT_LOG_LEVEL = "INFO"
_VALID_LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}


@dataclass(frozen=True, slots=True)
class Settings:
    idempotency_ttl_seconds: float = DEFAULT_IDEMPOTENCY_TTL_SECONDS
    processing_delay_seconds: float = DEFAULT_PROCESSING_DELAY_SECONDS
    max_request_body_bytes: int = DEFAULT_MAX_REQUEST_BODY_BYTES
    log_level: str = DEFAULT_LOG_LEVEL

    def __post_init__(self) -> None:
        if not math.isfinite(self.idempotency_ttl_seconds) or self.idempotency_ttl_seconds <= 0:
            raise ValueError("idempotency_ttl_seconds must be a finite number > 0")
        if not math.isfinite(self.processing_delay_seconds) or self.processing_delay_seconds < 0:
            raise ValueError("processing_delay_seconds must be a finite number >= 0")
        if self.max_request_body_bytes <= 0:
            raise ValueError("max_request_body_bytes must be > 0")
        if self.log_level.upper() not in _VALID_LOG_LEVELS:
            raise ValueError(f"log_level must be one of {sorted(_VALID_LOG_LEVELS)}, got {self.log_level!r}")

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> Settings:
        environ = os.environ if environ is None else environ
        return cls(
            idempotency_ttl_seconds=_read_float(
                environ, "IDEMPOTENCY_TTL_SECONDS", DEFAULT_IDEMPOTENCY_TTL_SECONDS
            ),
            processing_delay_seconds=_read_float(
                environ, "PAYMENT_PROCESSING_DELAY_SECONDS", DEFAULT_PROCESSING_DELAY_SECONDS
            ),
            max_request_body_bytes=_read_int(
                environ, "MAX_REQUEST_BODY_BYTES", DEFAULT_MAX_REQUEST_BODY_BYTES
            ),
            log_level=(environ.get("LOG_LEVEL", "").strip() or DEFAULT_LOG_LEVEL).upper(),
        )


def _read_float(environ: Mapping[str, str], name: str, default: float) -> float:
    raw = environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        raise ValueError(f"{name} must be a number, got {raw!r}") from None


def _read_int(environ: Mapping[str, str], name: str, default: int) -> int:
    raw = environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from None
