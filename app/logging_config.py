"""Structured-ish logging setup for the gateway's audit trail.

Payment amounts and currencies are not sensitive on their own (there is no card or
account data in this service), but the raw request body is still never logged: only
the idempotency key, a short fingerprint prefix and the outcome of the request.
"""

from __future__ import annotations

import logging

LOGGER_NAME = "idempotency_gateway"

_LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"


def configure_logging(level: str) -> None:
    logging.basicConfig(level=level, format=_LOG_FORMAT)
    logging.getLogger(LOGGER_NAME).setLevel(level)


def get_logger() -> logging.Logger:
    return logging.getLogger(LOGGER_NAME)
