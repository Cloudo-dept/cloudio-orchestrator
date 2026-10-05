"""Legacy automation runner mock: accepts a request, acknowledges it, and says nothing more.

Fire-and-forget by design — there is no status route to poll, because the real runner has none.
That absence is the point: a test cannot accidentally assert on an outcome the orchestrator could
never observe.
"""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from fastapi import FastAPI, Header, Request, Response

from tests.mocks.base import Override, apply_overrides


@dataclass
class LegacyRunnerMock:
    submit_path: str = "/api/v1/requests"
    # Every accepted request body, in order, and the Idempotency-Key it arrived under.
    submissions: list[dict[str, Any]] = field(default_factory=list)
    keys: list[str] = field(default_factory=list)
    _by_key: dict[str, str] = field(default_factory=dict)  # Idempotency-Key → reference
    # What the acknowledgement carries. Set to {} to model a runner that answers with nothing
    # the adapter recognises — which must still count as accepted.
    ack_reference_key: str | None = "request_id"
    overrides: list[Override] = field(default_factory=list)
    requests: list[tuple[str, str]] = field(default_factory=list)

    @property
    def app(self) -> FastAPI:
        return _build(self)


def _build(mock: LegacyRunnerMock) -> FastAPI:
    app = FastAPI()

    @app.middleware("http")
    async def _record_override(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        mock.requests.append((request.method, request.url.path))
        forced = apply_overrides(mock.overrides, request)
        return forced if forced is not None else await call_next(request)

    @app.post(mock.submit_path)
    async def submit(
        body: dict[str, Any],
        idempotency_key: str = Header(alias="Idempotency-Key"),
    ) -> dict[str, Any]:
        mock.keys.append(idempotency_key)
        if idempotency_key in mock._by_key:  # replay → the same reference, no second request
            reference = mock._by_key[idempotency_key]
        else:
            mock.submissions.append(body)
            reference = f"legacy-{len(mock.submissions)}"
            mock._by_key[idempotency_key] = reference
        if mock.ack_reference_key is None:
            return {"message": "accepted"}  # nothing the adapter can pick a reference out of
        return {"message": "accepted", mock.ack_reference_key: reference}

    return app
