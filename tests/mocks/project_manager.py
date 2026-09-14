"""Stateful Project Manager mock: idempotent create + patch + delete, with a request/patch log."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Request, Response

from tests.mocks.base import Override, apply_overrides


@dataclass
class ProjectManagerMock:
    resources: dict[str, dict[str, Any]] = field(default_factory=dict)  # key: project/type/vendor
    _by_key: dict[str, str] = field(default_factory=dict)  # Idempotency-Key → res key
    patches: list[dict[str, Any]] = field(default_factory=list)
    deletes: list[str] = field(default_factory=list)  # resource keys, in order
    overrides: list[Override] = field(default_factory=list)
    requests: list[tuple[str, str]] = field(default_factory=list)

    @property
    def app(self) -> FastAPI:
        return _build(self)


def _build(mock: ProjectManagerMock) -> FastAPI:
    app = FastAPI()

    @app.middleware("http")
    async def _record_override(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        mock.requests.append((request.method, request.url.path))
        forced = apply_overrides(mock.overrides, request)
        return forced if forced is not None else await call_next(request)

    @app.post("/projects/{project_id}/project_resources/{resource_type}")
    async def create(
        project_id: str,
        resource_type: str,
        body: dict[str, Any],
        idempotency_key: str = Header(alias="Idempotency-Key"),
    ) -> dict[str, Any]:
        if idempotency_key in mock._by_key:  # replay → same resource
            return mock.resources[mock._by_key[idempotency_key]]
        key = f"{project_id}/{resource_type}/{body['vendor_id']}"
        mock.resources[key] = {**body, "in_progress": True}
        mock._by_key[idempotency_key] = key
        return mock.resources[key]

    @app.patch("/projects/{project_id}/project_resources/{resource_type}/{vendor_id}")
    async def update(
        project_id: str, resource_type: str, vendor_id: str, body: dict[str, Any]
    ) -> dict[str, Any]:
        key = f"{project_id}/{resource_type}/{vendor_id}"
        mock.resources[key].update(body)
        mock.patches.append({"vendor_id": vendor_id, **body})
        return mock.resources[key]

    @app.delete(
        "/projects/{project_id}/project_resources/{resource_type}/{vendor_id}", status_code=204
    )
    async def delete(project_id: str, resource_type: str, vendor_id: str) -> None:
        key = f"{project_id}/{resource_type}/{vendor_id}"
        if mock.resources.pop(key, None) is None:  # already gone → 404, as the real provider does
            raise HTTPException(404, detail=f"resource '{vendor_id}' not found")
        mock.deletes.append(key)

    return app
