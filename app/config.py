"""Runtime configuration loaded from environment variables."""

from __future__ import annotations

import math
import os
from collections.abc import Mapping
from dataclasses import dataclass

DEFAULT_IDEMPOTENCY_TTL_SECONDS = 86_400.0  # 24 hours
DEFAULT_PROCESSING_DELAY_SECONDS = 2.0


@dataclass(frozen=True, slots=True)
class Settings:
    idempotency_ttl_seconds: float = DEFAULT_IDEMPOTENCY_TTL_SECONDS
    processing_delay_seconds: float = DEFAULT_PROCESSING_DELAY_SECONDS

    def __post_init__(self) -> None:
        if not math.isfinite(self.idempotency_ttl_seconds) or self.idempotency_ttl_seconds <= 0:
            raise ValueError("idempotency_ttl_seconds must be a finite number > 0")
        if not math.isfinite(self.processing_delay_seconds) or self.processing_delay_seconds < 0:
            raise ValueError("processing_delay_seconds must be a finite number >= 0")

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
        )


def _read_float(environ: Mapping[str, str], name: str, default: float) -> float:
    raw = environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        raise ValueError(f"{name} must be a number, got {raw!r}") from None
