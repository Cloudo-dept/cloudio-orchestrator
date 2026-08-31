*[← Index](README.md)*

# External Clients (ServiceNow · Airflow · Project Manager)

Three provider-agnostic ports, each carrying **only the methods orchestration actually calls**
(YAGNI: `open_work_item` and `put_resource` from the previous revision had no caller and are gone
— an adapter can grow provider-specific extras the day a use case exists). No provider vocabulary
(tables, sys_ids, numeric states, DAG params) crosses a port boundary; swapping a provider means
one new adapter module + one line in `bootstrap.py`.

## `ports.py` — the client contracts

```python
import abc
from typing import Any

from orchestrator.domain import EngineFailure, EngineRunStatus, TicketRef


class TicketSystemClient(abc.ABC):
    """Domain-level ticket operations (ServiceNow is one implementation)."""

    @abc.abstractmethod
    async def open_ticket(self, template_id: str, fields: dict[str, Any], requested_by: str,
                          idempotency_key: str) -> TicketRef:
        """Open a ticket from a provider template (idempotent on the key). `fields` is the
        caller's free-form template payload, passed through to the provider."""

    @abc.abstractmethod
    async def close_ticket(self, ticket: TicketRef, note: str | None = None,
                           outcome: TicketOutcome = TicketOutcome.SUCCESSFUL) -> None:
        """Close the ticket, optionally attaching a note. `outcome` says whether the request was
        fulfilled — a run that failed closes its ticket UNSUCCESSFUL, so a failed request is not
        indistinguishable from a completed one."""

    @abc.abstractmethod
    async def annotate_ticket(self, ticket: TicketRef, note: str) -> None:
        """Attach a note to the ticket without changing its state."""

    @abc.abstractmethod
    async def open_incident(self, summary: str, requested_by: str, responsible_group: str,
                            flow_type: str | None = None, failed_task: str | None = None,
                            comment: str | None = None,
                            description: str | None = None) -> TicketRef:
        """Raise an incident for a failure, routed to the responsible group. `summary` is the
        one-line title, `description` the body a responder reads first, and `comment` the same
        failure detail as a note — so it survives where a provider treats the body as immutable."""


class ResourceManagerClient(abc.ABC):
    """Domain-level resource operations (Project Manager is one implementation)."""

    @abc.abstractmethod
    async def create_resource(self, project_id: str, resource_type: str,
                              body: dict[str, Any], idempotency_key: str) -> dict[str, Any]:
        """Create a project resource; body carries vendor_id/name/region/etc."""

    @abc.abstractmethod
    async def update_resource(self, project_id: str, resource_type: str, vendor_id: str,
                              fields: dict[str, Any]) -> None:
        """Partial update (only changed fields) — e.g. in_progress=False on finalize.
        (There is no delete endpoint in the provider; nothing is ever removed.)"""


class WorkflowEngineClient(abc.ABC):
    """Domain-level engine operations (Airflow is one implementation)."""

    @abc.abstractmethod
    async def trigger_workflow(self, automation_id: str, params: dict[str, Any],
                               idempotency_key: str) -> str:
        """Start a run (idempotent on the key) and return the engine's run id."""

    @abc.abstractmethod
    async def query_run_status(self, automation_id: str, run_id: str) -> EngineRunStatus: ...

    @abc.abstractmethod
    async def get_failure(self, automation_id: str, run_id: str) -> EngineFailure:
        """Return typed failure detail for a failed run (task, responsible group, message)."""

    @abc.abstractmethod
    async def get_output(self, automation_id: str, run_id: str, key: str) -> str | None:
        """Return a named output value the run produced (Airflow: an XCom), or None if the
        run produced no output under that key. Best-effort enrichment — not every run has one."""
```

Engines are dispatched through a plain mapping — `Mapping[WorkflowEngineType,
WorkflowEngineClient]` built in `bootstrap.py`. (The previous revision's
`WorkflowEngineRegistry` class wrapped a dict lookup; the dict is enough.)

## `adapters/airflow.py`

REST API v2, token auth, `verify=False` per the plugin spec. `automation_id` == DAG id;
the engine run id == `dag_run_id`.

`get_failure` reads what the **DAG** published: the failure detail behind an incident comes from an
XCom named `exception_type` on the failed task instance, carrying
`{message, exception, responsible_group}`. Publishing it is the DAG author's side of the contract —
[`airflow/dags/cloudio_callbacks.py`](../airflow/dags/cloudio_callbacks.py) ships the task-level
`on_failure_callback` (`exception_callback(...)`) that does it, plus the DAG-level
`notify_orchestrator` wake-early callback. Airflow wraps an XCom entry in `{"key", "value", ...}`
and hands the value back stringified, so the callback pushes JSON and `_failure_from_xcom` parses
it. A DAG that publishes nothing is not an error — the entry 404s, the incident still opens, and it
falls back to the default team with no message.

`exception` (the class name of what the task raised) is what **classifies** the failure. The payload
schema is fixed, so the name is the only thing there is to classify by, and `_FAILURE_KINDS` maps it
to a `FailureKind` — the sole piece of that vocabulary that crosses the port. An unrecognised or
missing name stays `TASK`, so an unclassified DAG failure still opens an incident; the
[failure policy](07-orchestration.md) then decides retries, incident, and what the requester is told.

```python
import json
from typing import Any

import httpx

from orchestrator.domain import EngineFailure, EngineRunStatus
from orchestrator.ports import WorkflowEngineClient


# The exception classes a CloudIO DAG raises, mapped to the domain classification. Matching is by
# exact class name — the payload carries a name, not a type.
_FAILURE_KINDS: dict[str, FailureKind] = {
    "ValidationException": FailureKind.VALIDATION,
    "InfraPrecheckException": FailureKind.INFRA_PRECHECK,
    "TaskException": FailureKind.TASK,
}


def _failure_from_xcom(failed_task: str, body: Any) -> EngineFailure:
    """The {message, exception, responsible_group} payload the DAG's failure callback pushed.
    Anything that is not that shape degrades to a bare failure rather than raising."""
    value: Any = body.get("value") if isinstance(body, dict) else None
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:             # a plain string value is the message itself
            return EngineFailure(failed_task=failed_task, detail=value)
    if not isinstance(value, dict):
        return EngineFailure(failed_task=failed_task)
    raw_name = value.get("exception")
    exception_name = raw_name if isinstance(raw_name, str) else None
    return EngineFailure(failed_task=failed_task,
                         responsible_group=value.get("responsible_group"),
                         detail=value.get("message"),
                         exception_name=exception_name,
                         kind=_FAILURE_KINDS.get(exception_name or "", FailureKind.TASK))


class AirflowWorkflowEngineClient(WorkflowEngineClient):
    """Auth uses a bearer token from /auth/token, cached and refreshed on 401."""

    def __init__(self, base_url: str, username: str, password: str, timeout: float = 10.0,
                 transport: httpx.AsyncBaseTransport | None = None) -> None:
        self._base = base_url.rstrip("/")
        self._username = username
        self._password = password
        self._timeout = timeout
        self._token: str | None = None
        self._transport = transport     # test seam: inject an ASGITransport (11-testing); None in prod

    def _client(self, token: str | None) -> httpx.AsyncClient:
        headers = {"Accept": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return httpx.AsyncClient(base_url=self._base, timeout=self._timeout, verify=False,
                                 headers=headers, transport=self._transport)  # verify=False per spec

    async def _authenticate(self) -> str:
        async with self._client(None) as client:
            resp = await client.post("/auth/token",
                                     json={"username": self._username, "password": self._password})
            resp.raise_for_status()
            data = resp.json()
            self._token = data.get("access_token") or data["token"]
            return self._token

    async def _request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        token = self._token or await self._authenticate()
        async with self._client(token) as client:
            resp = await client.request(method, path, **kwargs)
        if resp.status_code == 401:                 # token expired -> re-auth once
            token = await self._authenticate()
            async with self._client(token) as client:
                resp = await client.request(method, path, **kwargs)
        return resp

    async def trigger_workflow(self, automation_id: str, params: dict[str, Any],
                               idempotency_key: str) -> str:
        # Body is {"conf": ...}; dag_run_id doubles as the at-least-once idempotency key.
        resp = await self._request(
            "POST", f"/api/v2/dags/{automation_id}/dagRuns",
            json={"dag_run_id": idempotency_key, "conf": params},
        )
        if resp.status_code == 409:                 # duplicate dag_run_id -> already triggered
            return idempotency_key
        if resp.status_code == 404:
            raise RuntimeError(f"DAG '{automation_id}' not found.")
        resp.raise_for_status()
        return resp.json()["dag_run_id"]

    async def query_run_status(self, automation_id: str, run_id: str) -> EngineRunStatus:
        resp = await self._request("GET", f"/api/v2/dags/{automation_id}/dagRuns/{run_id}")
        resp.raise_for_status()
        state = resp.json().get("state")            # queued | running | success | failed
        return {"success": EngineRunStatus.SUCCESS,
                "failed": EngineRunStatus.FAILED}.get(state, EngineRunStatus.IN_PROGRESS)

    async def get_failure(self, automation_id: str, run_id: str) -> EngineFailure:
        # Airflow needs two calls (failed task instances, then the task's exception XCom);
        # the port exposes ONE typed result — the two-call dance is an Airflow detail.
        resp = await self._request(
            "GET", f"/api/v2/dags/{automation_id}/dagRuns/{run_id}/taskInstances",
            params={"state": "failed"})
        resp.raise_for_status()
        tasks = [ti["task_id"] for ti in resp.json().get("task_instances", [])]
        if not tasks:
            return EngineFailure()
        exc = await self._request(
            "GET",
            f"/api/v2/dags/{automation_id}/dagRuns/{run_id}/taskInstances/{tasks[0]}"
            f"/xcomEntries/exception_type")
        if exc.status_code == 404:      # the task died before its failure callback published
            return EngineFailure(failed_task=tasks[0])
        exc.raise_for_status()
        return _failure_from_xcom(tasks[0], exc.json())

    async def get_output(self, automation_id: str, run_id: str, key: str) -> str | None:
        # XComs live on task instances; probe each task of the run for an entry named `key`.
        resp = await self._request(
            "GET", f"/api/v2/dags/{automation_id}/dagRuns/{run_id}/taskInstances")
        resp.raise_for_status()
        for ti in resp.json().get("task_instances", []):
            entry = await self._request(
                "GET",
                f"/api/v2/dags/{automation_id}/dagRuns/{run_id}"
                f"/taskInstances/{ti['task_id']}/xcomEntries/{key}")
            if entry.status_code == 404:             # this task did not push that key
                continue
            entry.raise_for_status()
            value = entry.json().get("value")
            if value is not None:
                return str(value)
        return None
```

## `adapters/servicenow.py`

Templates are catalog items, tickets are RITMs (`sc_req_item`), incidents are INCs. All ServiceNow
vocabulary is confined to this class. Create idempotency is orchestrator-added: the RITM is tagged
with `correlation_id` and looked up before re-ordering.

**Reference fields carry sys_ids, never names.** Teams and users cross the port as *names* — the
`approval_group` variable inside the caller's `ticket_params` (whose approval the RITM needs), the
`responsible_group` from the DAG that failed (routes the INC), a login for the requester — and this
class resolves them:
`_group_sys_id` checks the configured `responsible_groups` map (which is also its memo cache, and
serves both kinds of name) and otherwise looks the name up in `sys_user_group`, remembering it;
`_user_sys_id` does the same against `sys_user`. A name that resolves nowhere is never written into
the field: an incident falls back to the default incident team and, failing that, opens unassigned
for ServiceNow to triage; an unresolvable `approval_group` variable is ordered with the name the
caller wrote, since silently dropping one of their variables is worse than letting ServiceNow
reject it. `caller_id` / `sysparm_requested_for` likewise fall back to the raw login, which
ServiceNow accepts.

```python
from typing import Any

import httpx

from orchestrator.domain import TicketRef
from orchestrator.ports import TicketSystemClient


class ServiceNowTicketClient(TicketSystemClient):
    _BUSINESS_SERVICE = "רשת יחידה"
    _SERVICE_OFFERING = "שירותי פיתוח"
    _RITM_CLOSED = 3                # Closed Complete
    _RITM_CLOSED_INCOMPLETE = 4     # Closed Incomplete — the request ended in failure
    _CLOSE_STATES = {TicketOutcome.SUCCESSFUL: _RITM_CLOSED,
                     TicketOutcome.UNSUCCESSFUL: _RITM_CLOSED_INCOMPLETE}

    def __init__(self, base_url: str, username: str, password: str,
                 responsible_groups: dict[str, str], default_group: str, timeout: float = 10.0,
                 transport: httpx.AsyncBaseTransport | None = None) -> None:
        self._base = base_url.rstrip("/")
        self._auth = (username, password)
        self._groups = dict(responsible_groups)   # group name -> sys_id; also the memo cache
        self._default_group = default_group       # incident fallback when a name resolves nowhere
        self._timeout = timeout
        self._transport = transport     # test seam: inject an ASGITransport (11-testing); None in prod

    def _client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(base_url=self._base, timeout=self._timeout, auth=self._auth,
                                 headers={"Accept": "application/json"}, transport=self._transport)

    async def _group_sys_id(self, name: str) -> str | None:
        """The configured map first, then ServiceNow. None when the name resolves nowhere — the
        caller decides; never write the name itself into a reference field."""
        known = self._groups.get(name)
        if known:
            return known
        sys_id = await self._lookup_sys_id("sys_user_group", "name", name)
        if sys_id is None:
            logger.warning("No ServiceNow group named '%s'.", name)
            return None
        self._groups[name] = sys_id             # memoised; a miss is not, so a new group resolves
        return sys_id

    async def _lookup_sys_id(self, table: str, field: str, value: str) -> str | None:
        """The sys_id of the first row where `field` == `value`, or None. Every reference field
        (assignment_group, caller_id, ...) is filled from this."""
        async with self._client() as client:
            resp = await client.get(f"/api/now/table/{table}",
                                    params={"sysparm_query": f"{field}={value}",
                                            "sysparm_fields": "sys_id", "sysparm_limit": 1})
            resp.raise_for_status()
            rows = resp.json().get("result", [])
        return str(rows[0]["sys_id"]) if rows else None

    async def _user_sys_id(self, login: str) -> str:
        """The sys_user sys_id for a login, falling back to the login itself when unknown."""
        sys_id = await self._lookup_sys_id("sys_user", "user_param", login)
        return sys_id if sys_id is not None else login

    async def _patch(self, table: str, sys_id: str, **body: Any) -> None:
        async with self._client() as client:
            resp = await client.patch(f"/api/now/table/{table}/{sys_id}", json=body)
            resp.raise_for_status()

    async def _find_ritm(self, client: httpx.AsyncClient, key: str) -> TicketRef | None:
        resp = await client.get("/api/now/table/sc_req_item",
                                params={"sysparm_query": f"correlation_id={key}",
                                        "sysparm_fields": "number,sys_id", "sysparm_limit": 1})
        resp.raise_for_status()
        rows = resp.json().get("result", [])
        return TicketRef(ticket_id=rows[0]["number"], native_id=rows[0]["sys_id"]) if rows else None

    async def _resolved_variables(self, fields: dict[str, Any]) -> dict[str, Any]:
        """The caller's catalog variables, with the approval group turned into a sys_id. A copy:
        the original is the run's persisted ticket_params. A name that resolves nowhere is sent as
        it came — deleting a caller's variable is worse than letting ServiceNow reject it."""
        name = fields.get(APPROVAL_GROUP_VARIABLE)
        if not isinstance(name, str) or not name:
            return fields
        sys_id = await self._group_sys_id(name)
        if sys_id is None:
            logger.warning("Ordering with %s='%s' unresolved.", APPROVAL_GROUP_VARIABLE, name)
            return fields
        return fields | {APPROVAL_GROUP_VARIABLE: sys_id}

    async def open_ticket(self, template_id: str, fields: dict[str, Any], requested_by: str,
                          idempotency_key: str) -> TicketRef:
        # template_id == catalog item sys_id; ticket == RITM.
        async with self._client() as client:
            found = await self._find_ritm(client, idempotency_key)
            if found:                                      # already ordered for this key -> idempotent
                return found
            # Both lookups happen BEFORE ordering: every call between the order and the
            # correlation_id tag widens the double-order window (01-external-contracts).
            requested_by_sys_id = await self._user_sys_id(requested_by)
            variables = await self._resolved_variables(fields)
            order = await client.post(
                f"/api/sn_sc/servicecatalog/items/{template_id}/order_now",
                json={"variables": variables, "sysparm_quantity": 1,
                      "sysparm_requested_for": requested_by_sys_id})
            order.raise_for_status()
            request_sys_id = order.json()["result"]["sys_id"]
            ritm = await client.get("/api/now/table/sc_req_item",
                                    params={"sysparm_query": f"request={request_sys_id}",
                                            "sysparm_fields": "number,sys_id", "sysparm_limit": 1})
            ritm.raise_for_status()
            r = ritm.json()["result"][0]
            await client.patch(f"/api/now/table/sc_req_item/{r['sys_id']}",
                               json={"correlation_id": idempotency_key})
            return TicketRef(ticket_id=r["number"], native_id=r["sys_id"])

    async def close_ticket(self, ticket: TicketRef, note: str | None = None,
                           outcome: TicketOutcome = TicketOutcome.SUCCESSFUL) -> None:
        body = {"work_notes": note} if note else {}
        await self._patch("sc_req_item", ticket.native_id,
                          state=self._CLOSE_STATES[outcome], **body)

    async def annotate_ticket(self, ticket: TicketRef, note: str) -> None:
        await self._patch("sc_req_item", ticket.native_id, work_notes=note)

    async def open_incident(self, summary: str, requested_by: str, responsible_group: str,
                            flow_type: str | None = None,
                            failed_task: str | None = None) -> TicketRef:
        body: dict[str, Any] = {
            "u_noc": True,
            "contact_type": "self-service",
            "short_description": summary,
            "urgency": 3, "impact": 3,
            "caller_id": await self._user_sys_id(requested_by),
            "business_service": self._BUSINESS_SERVICE,
            "service_offering": self._SERVICE_OFFERING,
            "u_new_subcategory": "CloudIO",
        }
        if description:                                    # what failed and why, as the INC body
            body["description"] = description
        # The responsible team, else the default incident team; omitted when neither resolves, so
        # ServiceNow triages it instead of receiving a broken reference.
        group_sys_id = await self._assignment_group(responsible_group)
        if group_sys_id is not None:
            body["assignment_group"] = group_sys_id
        if flow_type and failed_task:                      # DAG-run failures only
            body["u_cloudio_flow_type"] = flow_type
            body["u_cloudio_failed_task"] = failed_task
        async with self._client() as client:
            resp = await client.post("/api/now/table/incident", json=body)
            resp.raise_for_status()
            r = resp.json()["result"]
            return TicketRef(ticket_id=r["number"], native_id=r["sys_id"])
```

## `adapters/project_manager.py`

```python
from typing import Any

import httpx

from orchestrator.ports import ResourceManagerClient


class ProjectManagerResourceClient(ResourceManagerClient):
    """NOTE: the plugin doc lists Patch with method GET — treated here as PATCH."""

    def __init__(self, base_url: str, token: str, timeout: float = 10.0,
                 transport: httpx.AsyncBaseTransport | None = None) -> None:
        self._base = base_url.rstrip("/")
        self._token = token
        self._timeout = timeout
        self._transport = transport     # test seam: inject an ASGITransport (11-testing); None in prod

    def _client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(base_url=self._base, timeout=self._timeout,
                                 headers={"Accept": "application/json",
                                          "Authorization": f"Bearer {self._token}"},
                                 transport=self._transport)

    async def create_resource(self, project_id: str, resource_type: str,
                              body: dict[str, Any], idempotency_key: str) -> dict[str, Any]:
        async with self._client() as client:
            resp = await client.post(
                f"/projects/{project_id}/project_resources/{resource_type}",
                json=body, headers={"Idempotency-Key": idempotency_key})  # header is orchestrator-added
            resp.raise_for_status()
            return resp.json()

    async def update_resource(self, project_id: str, resource_type: str, vendor_id: str,
                              fields: dict[str, Any]) -> None:
        async with self._client() as client:
            resp = await client.patch(
                f"/projects/{project_id}/project_resources/{resource_type}/{vendor_id}",
                json=fields)
            resp.raise_for_status()
```

## What got simpler here (vs. the previous revision)

- **`get_failed_tasks` + `get_task_exceptions` collapsed into one `get_failure()`** returning a
  typed `EngineFailure`. The two-call sequence was Airflow's shape leaking through the port; a
  Temporal or Jenkins adapter shouldn't have to fake "task lists".
- **`callback_url` dropped from `trigger_workflow`** — the webhook path was cut; polling is the
  completion signal. One less thing every future engine adapter must pretend to support.
- **`open_work_item` (SC Task) and `put_resource` (full replace) deleted** — nothing called them.
- **`query_run_status` returns `EngineRunStatus`**, not a bare string.
- **`WorkflowEngineRegistry` class deleted** — a `Mapping[WorkflowEngineType, WorkflowEngineClient]`
  does the job.
