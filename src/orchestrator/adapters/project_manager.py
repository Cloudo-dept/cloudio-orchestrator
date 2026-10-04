"""Project Manager resource adapter."""

from typing import Any

import httpx

from orchestrator.domain import (
    ResourceNotFoundError,
    ResourceValidationRequest,
    ResourceValidationResult,
)
from orchestrator.ports import ResourceManagerClient

# How validate_resource says no: the request's data does not pass validation (400), an
# update/delete names a record that does not exist (404), or a create names a name that already
# does (409). All three are verdicts on the request — identical on every re-ask — so each crosses
# the port as a refusal. Anything else (a 5xx, a timeout) is the check itself failing and is raised,
# for the caller to retry.
_REFUSAL_CODES = frozenset(
    {
        httpx.codes.BAD_REQUEST,
        httpx.codes.NOT_FOUND,
        httpx.codes.CONFLICT,
    }
)


def _refusal_reason(resp: httpx.Response) -> str:
    """The provider's explanation for a refusal, for the run's error record. The error body is the
    provider's own shape, so anything unexpected degrades to the bare status rather than raising —
    losing the wording must not turn a clean refusal into a failure."""
    try:
        body = resp.json()
    except ValueError:
        body = None
    if isinstance(body, dict):
        for key in ("detail", "message", "error"):
            value = body.get(key)
            if isinstance(value, str) and value:
                return value
    return f"validation refused with status {resp.status_code}"


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

    async def validate_resource(
        self, request: ResourceValidationRequest
    ) -> ResourceValidationResult:
        # The endpoint's own spelling of the question — flow_type / db_operation / variables — is
        # Project Manager's, so it is written here and nowhere else; the port speaks
        # workflow_identifier / operation / params. An absent region or environment is left out
        # rather than sent as null: both are optional, and a null is a value, not a silence.
        body: dict[str, Any] = {
            "flow_type": request.workflow_identifier,
            "db_operation": request.operation.value,
            "variables": request.params,
            "name": request.name,
        }
        if request.environment is not None:
            body["environment"] = request.environment
        if request.region is not None:
            body["region"] = request.region
        resp = await self._http.post(
            f"/projects/{request.project_id}/project_resources/{request.resource_type}"
            f"/validate_resource",
            json=body,
        )
        if resp.status_code in _REFUSAL_CODES:  # provider vocabulary stops here
            return ResourceValidationResult(eligible=False, reason=_refusal_reason(resp))
        resp.raise_for_status()
        return ResourceValidationResult(eligible=True)

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
