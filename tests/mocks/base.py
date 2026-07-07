"""Shared mock-server plumbing: declarative overrides + an in-process ASGI transport helper."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import httpx
from fastapi import Request, Response
from fastapi.responses import JSONResponse


@dataclass
class Override:
    """Force a response for any request whose path contains `path_contains`.
    The one knob for error-path tests — no route code changes."""

    path_contains: str
    status: int = 500
    json: Any = None
    method: str | None = None  # None = any method


def apply_overrides(overrides: list[Override], request: Request) -> JSONResponse | None:
    for o in overrides:
        if o.path_contains in request.url.path and o.method in (None, request.method):
            return JSONResponse(
                o.json if o.json is not None else {"detail": "forced"}, status_code=o.status
            )
    return None


Middleware = Callable[[Request, Callable[[Request], Awaitable[Response]]], Awaitable[Response]]


def asgi(app: Any) -> httpx.ASGITransport:
    """Route a real adapter's httpx client straight into a mock app — no socket."""
    return httpx.ASGITransport(app=app)
