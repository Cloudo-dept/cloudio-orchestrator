"""ServiceNow ticket-system adapter.

Templates are catalog items, tickets are RITMs (sc_req_item), incidents are INCs. All ServiceNow
vocabulary is confined to this class. Create idempotency is orchestrator-added: the RITM is tagged
with correlation_id and looked up before re-ordering.
"""

import logging
from typing import Any

import httpx

from orchestrator.domain import ApprovalStatus, TicketOutcome, TicketRef
from orchestrator.ports import TicketSystemClient

logger = logging.getLogger(__name__)

# The catalog variable naming the group whose approval the request needs. It arrives inside the
# caller's ticket_params, and this adapter resolves its value from a group name to a sys_id — the
# variable is a reference to sys_user_group. Must match the catalog item's variable exactly, or
# ServiceNow drops it without complaint.
APPROVAL_GROUP_VARIABLE = "approval_group"

# The columns a name is looked up by. Instance-specific: change them here, in one place, if your
# ServiceNow identifies groups or users by something other than these (u_group_name, user_name, an
# email). Everything a group or user name is resolved through goes via these two.
GROUP_LOOKUP_FIELD = "name"  # sys_user_group column matched against a group name
USER_LOOKUP_FIELD = "user_param"  # sys_user column matched against a login


class ServiceNowTicketClient(TicketSystemClient):
    _BUSINESS_SERVICE = "רשת יחידה"
    _SERVICE_OFFERING = "שירותי פיתוח"
    _RITM_CLOSED = 3  # Closed Complete
    _RITM_CLOSED_INCOMPLETE = 4  # Closed Incomplete — the request ended in failure
    # RITM `state` per close outcome: a failed run must not leave a RITM that reads as fulfilled.
    _CLOSE_STATES = {
        TicketOutcome.SUCCESSFUL: _RITM_CLOSED,
        TicketOutcome.UNSUCCESSFUL: _RITM_CLOSED_INCOMPLETE,
    }
    # RITM `approval` field values that are terminal; anything else means still pending.
    _APPROVAL_MAP = {"approved": ApprovalStatus.APPROVED, "rejected": ApprovalStatus.REJECTED}

    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        responsible_groups: dict[str, str],
        default_group: str,
        timeout: float = 10.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._base = base_url.rstrip("/")
        self._auth = (username, password)
        # group name -> sys_user_group sys_id. Seeded from config (an override for names that differ
        # from what a DAG raises, and a way to skip the lookup for hot names) and filled in by
        # _group_sys_id, so each name costs at most one lookup per process.
        self._groups = dict(responsible_groups)
        self._default_group = default_group  # incident fallback when a name resolves nowhere
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

    async def _group_sys_id(self, name: str) -> str | None:
        """The sys_user_group sys_id for a team name: the configured map first, then ServiceNow.

        A reference field must carry a sys_id, so a name that resolves nowhere returns None and the
        caller decides what to do — never write the name itself into the field. A resolved name is
        memoised; a miss is not, so a group created later resolves on the next attempt instead of
        staying broken until a restart.
        """
        known = self._groups.get(name)
        if known:
            return known
        sys_id = await self._lookup_sys_id("sys_user_group", GROUP_LOOKUP_FIELD, name)
        if sys_id is None:
            logger.warning("No ServiceNow group named '%s'.", name)
            return None
        self._groups[name] = sys_id
        logger.info("Resolved ServiceNow group '%s' to %s.", name, sys_id)
        return sys_id

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

    async def _lookup_sys_id(self, table: str, field: str, value: str) -> str | None:
        """The sys_id of the first row where `field` == `value`, or None when there is no such row.
        Every reference field (assignment_group, caller_id, ...) is filled from this."""
        async with self._client() as client:
            resp = await client.get(
                f"/api/now/table/{table}",
                params={
                    "sysparm_query": f"{field}={value}",
                    "sysparm_fields": "sys_id",
                    "sysparm_limit": 1,
                },
            )
            resp.raise_for_status()
            rows = resp.json().get("result", [])
        return str(rows[0]["sys_id"]) if rows else None

    async def _user_sys_id(self, login: str) -> str:
        """The sys_user sys_id for a login, falling back to the login itself when there is no such
        user — ServiceNow resolves some references by login, and a ticket opened against a slightly
        wrong caller beats no ticket at all."""
        sys_id = await self._lookup_sys_id("sys_user", USER_LOOKUP_FIELD, login)
        if sys_id is None:
            logger.warning("No ServiceNow user '%s'; using the login as-is.", login)
            return login
        return sys_id

    async def _resolved_variables(self, fields: dict[str, Any]) -> dict[str, Any]:
        """The caller's catalog variables, with the approval group turned into a sys_id.

        ``fields`` is a pass-through payload and stays untouched apart from that one variable: it
        names a group, and a reference variable needs the group's sys_id. A copy, because the
        original is the run's persisted ``ticket_params`` — rewriting it in place would overwrite
        what the caller asked for with a sys_id. A name that resolves nowhere is sent as it came:
        deleting a caller's variable is worse than letting ServiceNow reject it.
        """
        name = fields.get(APPROVAL_GROUP_VARIABLE)
        if not isinstance(name, str) or not name:
            return fields
        sys_id = await self._group_sys_id(name)
        if sys_id is None:
            logger.warning("Ordering with %s='%s' unresolved.", APPROVAL_GROUP_VARIABLE, name)
            return fields
        return fields | {APPROVAL_GROUP_VARIABLE: sys_id}

    async def open_ticket(
        self,
        template_id: str,
        fields: dict[str, Any],
        requested_by: str,
        idempotency_key: str,
    ) -> TicketRef:
        # template_id == catalog item sys_id; ticket == RITM.
        async with self._client() as client:
            found = await self._find_ritm(client, idempotency_key)
            if found:  # already ordered -> idempotent
                return found
            # Both lookups happen BEFORE ordering: every call between the order and the
            # correlation_id tag widens the window where a crash leaves an untagged RITM that the
            # next attempt cannot find and therefore double-orders (01-external-contracts).
            requested_by_sys_id = await self._user_sys_id(requested_by)
            variables = await self._resolved_variables(fields)
            order = await client.post(
                f"/api/sn_sc/servicecatalog/items/{template_id}/order_now",
                json={
                    "variables": variables,
                    "sysparm_quantity": 1,
                    "sysparm_requested_for": requested_by_sys_id,
                },
                timeout=120.0,
            )
            logger.info("Created a new request: %s", order.json())
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
            logger.info("Created RITM: %s", r["number"])

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
            logger.info("RITM Status: %s", resp.json())
            approval = resp.json()["result"].get("approval", "")
        return self._APPROVAL_MAP.get(approval, ApprovalStatus.PENDING)

    async def close_ticket(
        self,
        ticket: TicketRef,
        note: str | None = None,
        outcome: TicketOutcome = TicketOutcome.SUCCESSFUL,
    ) -> None:
        body = {"work_notes": note} if note else {}
        state = self._CLOSE_STATES[outcome]
        await self._patch("sc_req_item", ticket.native_id, state=state, **body)

    async def annotate_ticket(self, ticket: TicketRef, note: str) -> None:
        await self._patch("sc_req_item", ticket.native_id, work_notes=note)

    async def _assignment_group(self, responsible_group: str) -> str | None:
        """The group an incident is assigned to: the responsible team, else the default incident
        team. None when neither resolves — ServiceNow's own triage beats a broken reference."""
        sys_id = await self._group_sys_id(responsible_group)
        if sys_id is not None:
            return sys_id
        if responsible_group == self._default_group:
            logger.error("Default incident team '%s' did not resolve.", self._default_group)
            return None
        logger.warning(
            "Group '%s' did not resolve; falling back to the default team '%s'.",
            responsible_group,
            self._default_group,
        )
        fallback = await self._group_sys_id(self._default_group)
        if fallback is None:
            logger.error(
                "Default incident team '%s' did not resolve either; opening the incident "
                "unassigned.",
                self._default_group,
            )
        return fallback

    async def open_incident(
        self,
        summary: str,
        requested_by: str,
        responsible_group: str,
        flow_type: str | None = None,
        failed_task: str | None = None,
        comment: str | None = None,
        description: str | None = None,
    ) -> TicketRef:
        body: dict[str, Any] = {
            "u_noc": True,
            "contact_type": "self-service",
            "short_description": summary,
            "urgency": 3,
            "impact": 3,
            "caller_id": await self._user_sys_id(requested_by),
            "business_service": self._BUSINESS_SERVICE,
            "service_offering": self._SERVICE_OFFERING,
            "u_new_subcategory": "CloudIO",
        }
        group_sys_id = await self._assignment_group(responsible_group)
        if group_sys_id is not None:
            body["assignment_group"] = group_sys_id
        if description:  # the body a responder reads first: what failed and why
            body["description"] = description
        # Independent: a failure outside the engine still names the workflow it belongs to, even
        # when there is no task to name (and vice versa). Together they answer "which flow, and
        # what in it" on every incident, not just the ones a DAG raised.
        if flow_type:
            body["u_cloudio_flow_type"] = flow_type
        if failed_task:
            body["u_cloudio_failed_task"] = failed_task
        if comment:  # the same failure detail again, as a work note
            body["work_notes"] = comment
        async with self._client() as client:
            resp = await client.post("/api/now/table/incident", json=body)
            resp.raise_for_status()
            r = resp.json()["result"]
            return TicketRef(ticket_id=r["number"], native_id=r["sys_id"])
