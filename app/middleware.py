"""ASGI middleware for basic request hygiene."""

from __future__ import annotations

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import JSONResponse, Response


class MaxBodySizeMiddleware(BaseHTTPMiddleware):
    """Rejects requests whose declared ``Content-Length`` exceeds ``max_bytes``.

    A payment request is two small fields, so a client sending megabytes of JSON is
    either misbehaving or attempting a denial-of-service. Checking ``Content-Length``
    is a cheap, allocation-free guard; it does not defend against a body sent without
    that header, which is left to the ASGI server's own limits.
    """

    def __init__(self, app, max_bytes: int) -> None:
        super().__init__(app)
        self._max_bytes = max_bytes

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        content_length = request.headers.get("content-length")
        if content_length is not None:
            try:
                length = int(content_length)
            except ValueError:
                length = None
            if length is not None and length > self._max_bytes:
                return JSONResponse(
                    {"detail": f"Request body exceeds the {self._max_bytes}-byte limit."},
                    status_code=413,
                )
        return await call_next(request)
