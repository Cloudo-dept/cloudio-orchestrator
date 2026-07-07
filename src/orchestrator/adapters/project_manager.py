"""Project Manager resource adapter."""

from typing import Any

import httpx

from orchestrator.ports import ResourceManagerClient


class ProjectManagerResourceClient(ResourceManagerClient):
    """NOTE: the plugin doc lists Patch with method GET — treated here as PATCH."""

    def __init__(
        self,
        base_url: str,
        token: str,
        timeout: float = 10.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._base = base_url.rstrip("/")
        self._token = token
        self._timeout = timeout
        self._transport = transport  # test seam (11-testing); None in prod

    def _client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            base_url=self._base,
            timeout=self._timeout,
            headers={"Accept": "application/json", "Authorization": f"Bearer {self._token}"},
            transport=self._transport,
        )

    async def create_resource(
        self, project_id: str, resource_type: str, body: dict[str, Any], idempotency_key: str
    ) -> dict[str, Any]:
        async with self._client() as client:
            resp = await client.post(
                f"/projects/{project_id}/project_resources/{resource_type}",
                json=body,
                headers={"Idempotency-Key": idempotency_key},
            )  # orchestrator-added
            resp.raise_for_status()
            result: dict[str, Any] = resp.json()
            return result

    async def update_resource(
        self, project_id: str, resource_type: str, vendor_id: str, fields: dict[str, Any]
    ) -> None:
        async with self._client() as client:
            resp = await client.patch(
                f"/projects/{project_id}/project_resources/{resource_type}/{vendor_id}", json=fields
            )
            resp.raise_for_status()
