"""ServiceNow ticket-system adapter.

Templates are catalog items, tickets are RITMs (sc_req_item), incidents are INCs. All ServiceNow
vocabulary is confined to this class. Create idempotency is orchestrator-added: the RITM is tagged
with correlation_id and looked up before re-ordering.
"""

from typing import Any

import httpx

from orchestrator.domain import ApprovalStatus, TicketRef
from orchestrator.ports import TicketSystemClient


class ServiceNowTicketClient(TicketSystemClient):
    _BUSINESS_SERVICE = "רשת יחידה"
    _SERVICE_OFFERING = "שירותי פיתוח"
    _RITM_CLOSED = 3
    # RITM `approval` field values that are terminal; anything else means still pending.
    _APPROVAL_MAP = {"approved": ApprovalStatus.APPROVED, "rejected": ApprovalStatus.REJECTED}

    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        responsible_groups: dict[str, str],
        timeout: float = 10.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._base = base_url.rstrip("/")
        self._auth = (username, password)
        self._groups = responsible_groups
        self._timeout = timeout
        self._transport = transport  # test seam (11-testing); None in prod

    def _client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            base_url=self._base,
            timeout=self._timeout,
            auth=self._auth,
            headers={"Accept": "application/json"},
            transport=self._transport,
        )

    def _group(self, name: str) -> str:
        return self._groups.get(name, name)  # fall back to the exact name if unregistered

    async def _patch(self, table: str, sys_id: str, **body: Any) -> None:
        async with self._client() as client:
            resp = await client.patch(f"/api/now/table/{table}/{sys_id}", json=body)
            resp.raise_for_status()

    async def _find_ritm(self, client: httpx.AsyncClient, key: str) -> TicketRef | None:
        resp = await client.get(
            "/api/now/table/sc_req_item",
            params={
                "sysparm_query": f"correlation_id={key}",
                "sysparm_fields": "number,sys_id",
                "sysparm_limit": 1,
            },
        )
        resp.raise_for_status()
        rows = resp.json().get("result", [])
        return TicketRef(ticket_id=rows[0]["number"], native_id=rows[0]["sys_id"]) if rows else None

    async def open_ticket(
        self, template_id: str, fields: dict[str, Any], requested_by: str, idempotency_key: str
    ) -> TicketRef:
        # template_id == catalog item sys_id; ticket == RITM.
        async with self._client() as client:
            found = await self._find_ritm(client, idempotency_key)
            if found:  # already ordered -> idempotent
                return found
            order = await client.post(
                f"/api/sn_sc/servicecatalog/items/{template_id}/order_now",
                json={
                    "variables": fields,
                    "sysparm_quantity": 1,
                    "sysparm_requested_for": requested_by,
                },
            )
            order.raise_for_status()
            request_sys_id = order.json()["result"]["sys_id"]
            ritm = await client.get(
                "/api/now/table/sc_req_item",
                params={
                    "sysparm_query": f"request={request_sys_id}",
                    "sysparm_fields": "number,sys_id",
                    "sysparm_limit": 1,
                },
            )
            ritm.raise_for_status()
            r = ritm.json()["result"][0]
            await client.patch(
                f"/api/now/table/sc_req_item/{r['sys_id']}",
                json={"correlation_id": idempotency_key},
            )
            return TicketRef(ticket_id=r["number"], native_id=r["sys_id"])

    async def get_approval_status(self, ticket: TicketRef) -> ApprovalStatus:
        async with self._client() as client:
            resp = await client.get(
                f"/api/now/table/sc_req_item/{ticket.native_id}",
                params={"sysparm_fields": "approval"},
            )
            resp.raise_for_status()
            approval = resp.json()["result"].get("approval", "")
        return self._APPROVAL_MAP.get(approval, ApprovalStatus.PENDING)

    async def close_ticket(self, ticket: TicketRef, note: str | None = None) -> None:
        body = {"work_notes": note} if note else {}
        await self._patch("sc_req_item", ticket.native_id, state=self._RITM_CLOSED, **body)

    async def annotate_ticket(self, ticket: TicketRef, note: str) -> None:
        await self._patch("sc_req_item", ticket.native_id, work_notes=note)

    async def open_incident(
        self,
        summary: str,
        requested_by: str,
        responsible_group: str,
        flow_type: str | None = None,
        failed_task: str | None = None,
    ) -> TicketRef:
        body: dict[str, Any] = {
            "u_noc": True,
            "contact_type": "self-service",
            "short_descriptoin": summary,  # field name per the plugin spec (sic)
            "urgency": 3,
            "impact": 3,
            "caller_id": requested_by,
            "business_service": self._BUSINESS_SERVICE,
            "service_offering": self._SERVICE_OFFERING,
            "u_new_subcategory": "CloudIO",
            "assignment_group": self._group(responsible_group),
        }
        if flow_type and failed_task:  # DAG-run failures only
            body["u_cloudio_flow_type"] = flow_type
            body["u_cloudio_failed_task"] = failed_task
        async with self._client() as client:
            resp = await client.post("/api/now/table/incident", json=body)
            resp.raise_for_status()
            r = resp.json()["result"]
            return TicketRef(ticket_id=r["number"], native_id=r["sys_id"])
