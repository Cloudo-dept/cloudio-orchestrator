"""Project Manager resource adapter."""

from typing import Any

import httpx

from orchestrator.domain import ResourceNotFoundError
from orchestrator.ports import ResourceManagerClient


class ProjectManagerResourceClient(ResourceManagerClient):
    """NOTE: the plugin doc lists Patch with method GET — treated here as PATCH."""

    def __init__(self, client: httpx.AsyncClient) -> None:
        # One pooled client for the process, built and closed by the composition root — see the
        # note in ServiceNowTicketClient on why this is never a client per call.
        self._http = client

    async def create_resource(
        self, project_id: str, resource_type: str, body: dict[str, Any], idempotency_key: str
    ) -> str | None:
        resp = await self._http.post(
            f"/projects/{project_id}/project_resources/{resource_type}",
            json=body,
            headers={"Idempotency-Key": idempotency_key},
        )  # orchestrator-added
        resp.raise_for_status()
        # The create answers an acknowledgement — {message, project_resource_id} — not the record
        # it made. That spelling stops here: the port promises only "the provider's id for the
        # record", which is what the run stores.
        resource_id = resp.json().get("project_resource_id")
        return str(resource_id) if resource_id is not None else None

    async def update_resource(
        self, project_id: str, resource_type: str, vendor_id: str, fields: dict[str, Any]
    ) -> None:
        resp = await self._http.patch(
            f"/projects/{project_id}/project_resources/{resource_type}/{vendor_id}", json=fields
        )
        if resp.status_code == httpx.codes.NOT_FOUND:
            raise ResourceNotFoundError(
                f"No {resource_type} resource '{vendor_id}' in project '{project_id}'."
            )
        resp.raise_for_status()

    async def delete_resource(self, project_id: str, resource_type: str, vendor_id: str) -> None:
        resp = await self._http.delete(
            f"/projects/{project_id}/project_resources/{resource_type}/{vendor_id}"
        )
        # Already gone → the delete has happened; a re-driven finalize (delivery is
        # at-least-once) must not fail on the strength of its own earlier success.
        if resp.status_code == httpx.codes.NOT_FOUND:
            return
        resp.raise_for_status()
