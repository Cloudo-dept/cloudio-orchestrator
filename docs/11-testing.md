*[← Index](README.md)*

# Testing Strategy & Mock Harness

[10-verification](10-verification.md) lists **what** must hold. This page is **how** the suite is
built: the unit/integration split, the in-memory port fakes, and — the piece that carries the
provider integration tests — a **mock server per external API** (ServiceNow, Airflow, Project
Manager) that is faithful by default and easy to bend per test.

Two layers, two substitution points — matching the ports-vs-adapters seam:

| Layer | Substitutes at | Exercises | Speed | Tooling |
|---|---|---|---|---|
| **Unit** | the **port** (`TicketSystemClient`, …) | domain, step handlers, executor, worker loop, services | µs — no I/O | in-memory port **fakes** |
| **Integration** | the **HTTP wire** | the **real adapters** + the flow end-to-end | ms — ASGI in-proc | **mock servers** + real Postgres |

Rule of thumb: orchestration logic is tested against **fakes** (no serialization noise); the
adapters and the whole ticket→resource→engine→finalize flow are tested against **mock servers** so
real request-building, auth refresh, and idempotency lookups are covered.

## Test tree

```text
tests/
├── conftest.py                     # fixtures: mock instances, mock-wired real clients, pg_session_factory
├── mocks/                          # the mock servers — one FastAPI app per provider
│   ├── __init__.py
│   ├── base.py                     # Override + apply_overrides + MockServer (ephemeral-port option)
│   ├── servicenow.py               # ServiceNowMock
│   ├── airflow.py                  # AirflowMock
│   └── project_manager.py          # ProjectManagerMock
├── fakes.py                        # in-memory port implementations (clients, repos)
├── unit/
│   ├── test_domain.py              # enum/JSONB round-trip, RunState validation
│   ├── test_orchestration.py       # steps/executor/escalator over fakes; retry & idempotency
│   ├── test_worker.py              # RunWorker loop: claims 1, drives, empty-poll backoff
│   ├── test_services.py            # trigger → WorkflowRun built from the ResolvedWorkflow snapshot
│   └── test_dag_callbacks.py       # airflow/dags/cloudio_callbacks.py, loaded by path (stdlib only)
└── integration/
    ├── test_adapter_servicenow.py  # real ServiceNowTicketClient vs ServiceNowMock
    ├── test_adapter_airflow.py
    ├── test_adapter_project_manager.py
    ├── test_repository.py          # testcontainers Postgres: optimistic concurrency, claim_due/SKIP LOCKED, JSONB finders
    └── test_end_to_end.py          # full run over the 3 mocks + real Postgres
```

## The adapter test seam (one param)

The three HTTP adapters gain a single optional constructor argument — an httpx transport, `None` in
production — passed straight to the `httpx.AsyncClient` they build. This is the whole seam that lets
a **real adapter** talk to a **mock app** in-process, with no sockets and no monkeypatching:

```python
# adapters/*.py — the only change vs. 06-external-clients
def __init__(self, ..., transport: httpx.AsyncBaseTransport | None = None) -> None:
    ...
    self._transport = transport

def _client(self, ...) -> httpx.AsyncClient:
    return httpx.AsyncClient(base_url=self._base, ..., transport=self._transport)
```

It stays generic — a transport is httpx's own abstraction, no provider vocabulary — and honours the
DI rule: `bootstrap.py` passes `None`, tests pass an `ASGITransport`.

## Mock-server design — faithful defaults, declarative overrides

Every mock is the same three things, so learning one teaches all three:

1. **A typed in-memory state** (dataclasses) holding the provider's records — RITMs, DAG runs,
   resources — plus a `requests` audit log.
2. **A FastAPI app** that implements the provider's *real* routes over that state with correct
   happy-path semantics. Because the semantics are real (POST catalog → RITM exists → correlation
   lookup finds it), flows like ServiceNow's create-idempotency **just work** with zero per-test
   scripting.
3. **Two knobs to change behaviour** — mutate/seed state (`airflow.fail(...)`,
   `servicenow.seed_ritm(...)`), or attach an `Override` to force a status/body on any route (error
   paths) without touching route code.

### `mocks/base.py`

```python
from dataclasses import dataclass
from typing import Any

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse


@dataclass
class Override:
    """Force a response for any request whose path contains `path_contains`.
    The one knob for error-path tests — no route code changes."""
    path_contains: str
    status: int = 500
    json: Any = None
    method: str | None = None                       # None = any method


def apply_overrides(overrides: list[Override], request: Request) -> JSONResponse | None:
    for o in overrides:
        if o.path_contains in request.url.path and o.method in (None, request.method):
            return JSONResponse(o.json if o.json is not None else {"detail": "forced"},
                                status_code=o.status)
    return None


def asgi(app: FastAPI) -> httpx.ASGITransport:
    """Route a real adapter's httpx client straight into a mock app — no socket."""
    return httpx.ASGITransport(app=app)


class MockServer:
    """Optional: serve a mock app on an ephemeral localhost port (for black-box or
    subprocess-worker tests that need a real URL instead of an in-process transport)."""
    def __init__(self, app: FastAPI) -> None:
        import uvicorn
        self._config = uvicorn.Config(app, host="127.0.0.1", port=0, log_level="warning")
        self._server = uvicorn.Server(self._config)
        self.base_url: str = ""                     # set on start()
    # start()/stop() run self._server in a daemon thread and read back the bound port.
```

### `mocks/airflow.py`

```python
from dataclasses import dataclass, field
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .base import Override, apply_overrides


@dataclass
class DagRun:
    dag_run_id: str
    conf: dict[str, Any]
    state: str = "success"                          # queued | running | success | failed
    failed_task: str | None = None
    exception: dict[str, Any] = field(default_factory=dict)   # exception XCom; empty = none pushed


@dataclass
class AirflowMock:
    unknown_dags: set[str] = field(default_factory=set)       # DAGs that 404 on trigger
    runs: dict[str, DagRun] = field(default_factory=dict)     # keyed by dag_run_id
    default_state: str = "success"                            # state a freshly triggered run reports
    expire_token_once: bool = False                           # force one 401 to exercise re-auth
    overrides: list[Override] = field(default_factory=list)
    requests: list[tuple[str, str]] = field(default_factory=list)
    _token_spent: bool = False

    # --- scenario knobs ---
    def complete(self, dag_run_id: str, state: str = "success") -> None:
        self.runs[dag_run_id].state = state

    def fail(self, dag_run_id: str, *, task: str, responsible_group: str | None = None,
             message: str = "boom", exception: str = "TaskException",
             publish_exception: bool = True) -> None:
        """`exception` is the class name the DAG raised — what the orchestrator classifies the
        failure by. publish_exception=False models a task that died without its failure callback
        publishing the exception XCom (the entry then 404s, as in real Airflow)."""
        r = self.runs[dag_run_id]
        r.state, r.failed_task = "failed", task
        r.exception = ({"message": message, "exception": exception,
                        "responsible_group": responsible_group} if publish_exception else {})

    @property
    def app(self) -> FastAPI:
        return _build_airflow(self)


def _build_airflow(mock: AirflowMock) -> FastAPI:
    app = FastAPI()

    @app.middleware("http")
    async def _record_override_auth(request: Request, call_next):        # type: ignore[no-untyped-def]
        mock.requests.append((request.method, request.url.path))
        forced = apply_overrides(mock.overrides, request)
        if forced is not None:
            return forced
        if (mock.expire_token_once and not mock._token_spent
                and request.headers.get("authorization")):
            mock._token_spent = True                                     # one 401 → adapter re-auths
            return JSONResponse({"detail": "expired"}, status_code=401)
        return await call_next(request)

    @app.post("/auth/token")
    async def token() -> dict[str, str]:
        return {"access_token": "test-token"}

    @app.post("/api/v2/dags/{dag_id}/dagRuns")
    async def trigger(dag_id: str, body: dict[str, Any]) -> JSONResponse:
        if dag_id in mock.unknown_dags:
            return JSONResponse({"detail": "not found"}, status_code=404)
        run_id = body["dag_run_id"]
        if run_id in mock.runs:                                          # duplicate dag_run_id
            return JSONResponse({"detail": "duplicate"}, status_code=409)
        mock.runs[run_id] = DagRun(dag_run_id=run_id, conf=body.get("conf", {}),
                                   state=mock.default_state)
        return JSONResponse({"dag_run_id": run_id})

    @app.get("/api/v2/dags/{dag_id}/dagRuns/{run_id}")
    async def status(dag_id: str, run_id: str) -> dict[str, Any]:
        return {"state": mock.runs[run_id].state}

    @app.get("/api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances")
    async def failed_tasks(dag_id: str, run_id: str, state: str | None = None) -> dict[str, Any]:
        r = mock.runs[run_id]
        return {"task_instances": [{"task_id": r.failed_task}] if r.failed_task else []}

    @app.get("/api/v2/dags/{dag_id}/dagRuns/{run_id}"
             "/taskInstances/{task_id}/xcomEntries/exception_type")
    async def exception(dag_id: str, run_id: str, task_id: str) -> JSONResponse:
        exc = mock.runs[run_id].exception
        if not exc:                                  # the failing task published no exception XCom
            return JSONResponse({"detail": "not found"}, status_code=404)
        # Faithful shape: the entry is wrapped and the value comes back stringified — the DAG-side
        # failure callback pushes JSON, so the value is the JSON string the adapter parses.
        return JSONResponse({"key": "exception_type", "value": json.dumps(exc)})

    return app
```

### `mocks/servicenow.py`

```python
from dataclasses import dataclass, field
from typing import Any

from fastapi import FastAPI, Request

from .base import Override, apply_overrides


@dataclass
class Ritm:
    number: str
    sys_id: str
    request_sys_id: str
    correlation_id: str | None = None
    state: int | None = None
    work_notes: list[str] = field(default_factory=list)
    assignment_group: str | None = None        # sys_user_group sys_id the RITM was assigned to


@dataclass
class Incident:
    number: str
    sys_id: str
    body: dict[str, Any]


@dataclass
class ServiceNowMock:
    ritms: list[Ritm] = field(default_factory=list)
    incidents: list[Incident] = field(default_factory=list)
    overrides: list[Override] = field(default_factory=list)
    requests: list[tuple[str, str]] = field(default_factory=list)
    users: dict[str, str] = field(default_factory=lambda: {"jdoe": "usersys0000001"})
    # sys_user_group rows: name -> sys_id. What an unmapped group name resolves against; clear it
    # to model a name that exists nowhere. "cloudio" is the default incident team.
    groups: dict[str, str] = field(
        default_factory=lambda: {"CloudIO NetOps": "grpsys0000002", "cloudio": "grpsys0000003"})
    _seq: int = 0

    def _mint(self, prefix: str) -> tuple[str, str]:
        self._seq += 1
        return f"{prefix}{self._seq:07d}", f"{prefix.lower()}sys{self._seq:07d}"

    # scenario knob: pretend a RITM already exists for this idempotency key
    def seed_ritm(self, correlation_id: str) -> Ritm:
        number, sys_id = self._mint("RITM")
        r = Ritm(number=number, sys_id=sys_id, request_sys_id="req-seed",
                 correlation_id=correlation_id)
        self.ritms.append(r)
        return r

    @property
    def app(self) -> FastAPI:
        return _build_servicenow(self)


def _build_servicenow(mock: ServiceNowMock) -> FastAPI:
    app = FastAPI()

    @app.middleware("http")
    async def _record_override(request: Request, call_next):             # type: ignore[no-untyped-def]
        mock.requests.append((request.method, request.url.path))
        forced = apply_overrides(mock.overrides, request)
        return forced if forced is not None else await call_next(request)

    @app.get("/api/now/table/sc_req_item")
    async def query_ritm(sysparm_query: str = "") -> dict[str, Any]:
        # correlation_id=<key> (idempotency lookup) or request=<sys_id> (post-order resolve)
        field_name, _, value = sysparm_query.partition("=")
        if field_name == "correlation_id":
            hits = [r for r in mock.ritms if r.correlation_id == value]
        elif field_name == "request":
            hits = [r for r in mock.ritms if r.request_sys_id == value]
        else:
            hits = []
        return {"result": [{"number": r.number, "sys_id": r.sys_id} for r in hits[:1]]}

    @app.get("/api/now/table/sys_user_group")
    async def query_group(sysparm_query: str = "") -> dict[str, Any]:
        # name=<group name>; a group nobody created returns no rows, like the real table
        field_name, _, value = sysparm_query.partition("=")
        sys_id = mock.groups.get(value) if field_name == "name" else None
        return {"result": [{"sys_id": sys_id}] if sys_id else []}

    @app.post("/api/sn_sc/servicecatalog/items/{catalog_sys_id}/order_now")
    async def order(catalog_sys_id: str, body: dict[str, Any]) -> dict[str, Any]:
        number, sys_id = mock._mint("RITM")
        _, request_sys_id = mock._mint("REQ")
        mock.ritms.append(Ritm(number=number, sys_id=sys_id, request_sys_id=request_sys_id))
        return {"result": {"sys_id": request_sys_id}}

    @app.patch("/api/now/table/sc_req_item/{sys_id}")
    async def patch_ritm(sys_id: str, body: dict[str, Any]) -> dict[str, Any]:
        r = next(r for r in mock.ritms if r.sys_id == sys_id)
        if "correlation_id" in body:
            r.correlation_id = body["correlation_id"]
        if "state" in body:
            r.state = body["state"]
        if "work_notes" in body:
            r.work_notes.append(body["work_notes"])
        return {"result": {"number": r.number, "sys_id": r.sys_id}}

    @app.post("/api/now/table/incident")
    async def open_incident(body: dict[str, Any]) -> dict[str, Any]:
        number, sys_id = mock._mint("INC")
        mock.incidents.append(Incident(number=number, sys_id=sys_id, body=body))
        return {"result": {"number": number, "sys_id": sys_id}}

    return app
```

### `mocks/project_manager.py`

```python
from dataclasses import dataclass, field
from typing import Any

from fastapi import FastAPI, Header, Request

from .base import Override, apply_overrides


@dataclass
class ProjectManagerMock:
    resources: dict[str, dict[str, Any]] = field(default_factory=dict)   # key: project/type/vendor
    _by_key: dict[str, str] = field(default_factory=dict)                # Idempotency-Key → resource key
    patches: list[dict[str, Any]] = field(default_factory=list)
    overrides: list[Override] = field(default_factory=list)
    requests: list[tuple[str, str]] = field(default_factory=list)

    @property
    def app(self) -> FastAPI:
        return _build_pm(self)


def _build_pm(mock: ProjectManagerMock) -> FastAPI:
    app = FastAPI()

    @app.middleware("http")
    async def _record_override(request: Request, call_next):             # type: ignore[no-untyped-def]
        mock.requests.append((request.method, request.url.path))
        forced = apply_overrides(mock.overrides, request)
        return forced if forced is not None else await call_next(request)

    @app.post("/projects/{project_id}/project_resources/{resource_type}")
    async def create(project_id: str, resource_type: str, body: dict[str, Any],
                     idempotency_key: str = Header(alias="Idempotency-Key")) -> dict[str, Any]:
        if idempotency_key in mock._by_key:                              # replay → same resource
            return mock.resources[mock._by_key[idempotency_key]]
        key = f"{project_id}/{resource_type}/{body['vendor_id']}"
        mock.resources[key] = {**body, "in_progress": True}
        mock._by_key[idempotency_key] = key
        return mock.resources[key]

    @app.patch("/projects/{project_id}/project_resources/{resource_type}/{vendor_id}")
    async def update(project_id: str, resource_type: str, vendor_id: str,
                     body: dict[str, Any]) -> dict[str, Any]:
        key = f"{project_id}/{resource_type}/{vendor_id}"
        mock.resources[key].update(body)
        mock.patches.append({"vendor_id": vendor_id, **body})
        return mock.resources[key]

    return app
```

## Wiring — `conftest.py`

Each fixture yields the mock **and** a real adapter pointed at it through an `ASGITransport`. A test
touches the mock to arrange/assert and the adapter to act:

```python
import httpx
import pytest

from orchestrator.adapters.airflow import AirflowWorkflowEngineClient
from orchestrator.adapters.project_manager import ProjectManagerResourceClient
from orchestrator.adapters.servicenow import ServiceNowTicketClient
from tests.mocks.airflow import AirflowMock
from tests.mocks.base import asgi
from tests.mocks.project_manager import ProjectManagerMock
from tests.mocks.servicenow import ServiceNowMock


@pytest.fixture
def airflow() -> AirflowMock:
    return AirflowMock()


@pytest.fixture
def airflow_client(airflow: AirflowMock) -> AirflowWorkflowEngineClient:
    return AirflowWorkflowEngineClient(base_url="http://airflow.local", username="u",
                                       password="p", transport=asgi(airflow.app))


@pytest.fixture
def servicenow() -> ServiceNowMock:
    return ServiceNowMock()


@pytest.fixture
def servicenow_client(servicenow: ServiceNowMock) -> ServiceNowTicketClient:
    return ServiceNowTicketClient(base_url="http://servicenow.local", username="u", password="p",
                                  responsible_groups={"netops": "grpsys-netops"},
                                  default_group="cloudio",
                                  transport=asgi(servicenow.app))


@pytest.fixture
def project_manager() -> ProjectManagerMock:
    return ProjectManagerMock()


@pytest.fixture
def pm_client(project_manager: ProjectManagerMock) -> ProjectManagerResourceClient:
    return ProjectManagerResourceClient(base_url="http://pm.local", token="t",
                                        transport=asgi(project_manager.app))
```

## Adapter integration tests — examples

The happy paths need no scripting; error paths flip one knob:

```python
async def test_open_ticket_orders_then_returns_ritm(servicenow, servicenow_client):
    ref = await servicenow_client.open_ticket(template_id="cat-1", fields={"size": "L"},
                                              requested_by="jdoe",
                                              idempotency_key="run-1:creating_ticket")
    assert ref.ticket_id.startswith("RITM") and ref.native_id
    assert servicenow.ritms[-1].correlation_id == "run-1:creating_ticket"  # tagged for idempotency


async def test_open_ticket_is_idempotent_on_retry(servicenow, servicenow_client):
    servicenow.seed_ritm(correlation_id="run-1:creating_ticket")        # pretend already ordered
    await servicenow_client.open_ticket(template_id="cat-1", fields={}, requested_by="jdoe",
                                        idempotency_key="run-1:creating_ticket")
    assert not any(m == "POST" and "servicecatalog" in p for m, p in servicenow.requests)


async def test_get_failure_returns_typed_group(airflow, airflow_client):
    await airflow_client.trigger_workflow("dag-x", {}, "run-1")
    airflow.fail("run-1", task="provision_vm", responsible_group="netops", message="quota exceeded")
    failure = await airflow_client.get_failure("dag-x", "run-1")
    assert (failure.failed_task, failure.responsible_group, failure.detail) == \
           ("provision_vm", "netops", "quota exceeded")


@pytest.mark.parametrize(("exception", "kind"), [
    ("ValidationException", FailureKind.VALIDATION),
    ("InfraPrecheckException", FailureKind.INFRA_PRECHECK),
    ("TaskException", FailureKind.TASK),
    ("ValueError", FailureKind.TASK),                  # unclassified → the incident-opening default
])
async def test_get_failure_classifies_by_exception_name(exception, kind, airflow, airflow_client):
    await airflow_client.trigger_workflow("dag-x", {}, "run-1")
    airflow.fail("run-1", task="provision_vm", exception=exception, message="nope")
    failure = await airflow_client.get_failure("dag-x", "run-1")
    assert (failure.kind, failure.exception_name) == (kind, exception)


async def test_trigger_unknown_dag_raises(airflow, airflow_client):
    airflow.unknown_dags.add("ghost")
    with pytest.raises(RuntimeError, match="not found"):
        await airflow_client.trigger_workflow("ghost", {}, "run-1")


async def test_token_refresh_on_401(airflow, airflow_client):
    airflow.expire_token_once = True                                    # first authed call 401s
    await airflow_client.trigger_workflow("dag-x", {}, "run-1")         # transparently re-auths
    assert airflow.requests.count(("POST", "/auth/token")) == 2


async def test_finalize_patches_in_progress_false(project_manager, pm_client):
    await pm_client.create_resource("proj-1", "vm", {"vendor_id": "vm-1"}, "run-1:resource:0")
    await pm_client.update_resource("proj-1", "vm", "vm-1", {"in_progress": False})
    assert project_manager.patches[-1] == {"vendor_id": "vm-1", "in_progress": False}


async def test_create_resource_replay_is_idempotent(project_manager, pm_client):
    a = await pm_client.create_resource("proj-1", "vm", {"vendor_id": "vm-1"}, "same-key")
    b = await pm_client.create_resource("proj-1", "vm", {"vendor_id": "vm-1"}, "same-key")
    assert a == b and len(project_manager.resources) == 1


async def test_incident_open_5xx_is_surfaced(servicenow, servicenow_client):
    servicenow.overrides.append(Override(path_contains="/incident", status=500))
    with pytest.raises(httpx.HTTPStatusError):
        await servicenow_client.open_incident(summary="x", requested_by="jdoe",
                                              responsible_group="netops")
```

## End-to-end integration test

Assemble exactly what [bootstrap.py](08-entrypoints.md) builds, but with the three clients pointed at
the mocks and a **real Postgres** repository (the same `pg_session_factory` fixture the repository
tests use). Drive the run the way the worker does — `claim_due` → `executor.handle` — until terminal.
Poll intervals are set to `0` in the fixture so a re-claim is immediate, and
`airflow.default_state="success"` lets the engine step finish on its first poll:

```python
async def test_resource_run_reaches_completed(pg_session_factory, servicenow, airflow,
                                              project_manager):
    tickets = ServiceNowTicketClient(base_url="http://sn.local", username="u", password="p",
                                     responsible_groups={}, default_group="cloudio",
                                     transport=asgi(servicenow.app))
    engine = AirflowWorkflowEngineClient(base_url="http://af.local", username="u", password="p",
                                         transport=asgi(airflow.app))
    resources = ProjectManagerResourceClient(base_url="http://pm.local", token="t",
                                             transport=asgi(project_manager.app))
    # runs/workflows repos, handlers, executor, escalator and services assembled as in bootstrap.py
    ...

    await workflows.create(Workflow(identifier="provision-vm", run_type=RunType.RESOURCE,
                                    engine_type=WorkflowEngineType.AIRFLOW,
                                    automation_id="dag-x", ticket_template_id="cat-1"))
    run = await runs_service.trigger(
        workflow_identifier="provision-vm", created_by="jdoe", max_retries=3,
        ticket_params={}, workflow_params={},
        resource=ResourceSpec(project_id="proj-1", resource_type="vm", vendor_id="vm-1",
                              name="app-01", region="gvt", environment="prod"))

    for _ in range(12):                                     # stand in for the RunWorker loop
        due = await runs.claim_due(limit=1, lease_seconds=300)
        if not due:
            break
        for run_id in due:
            await executor.handle(run_id)

    final = await runs.get(run.run_id)
    assert final.status is RunStatus.COMPLETED
    assert any("servicecatalog" in p for _, p in servicenow.requests)   # ticket opened
    assert project_manager.patches[-1]["in_progress"] is False          # resource finalized
    assert servicenow.ritms[-1].state == 3                              # RITM closed
    assert not servicenow.incidents                                     # no failure → no INC
```

Swap in `airflow.fail(...)` before the loop (and let retries exhaust) to assert the **failure**
path: run ends `FAILED`, one `Incident` lands in `servicenow.incidents` routed to the responsible
group, a work note is on the RITM, and the resource is left `in_progress=False` — nothing rolled
back.

## Why this shape

- **The mock is the single source of truth** — one FastAPI app per provider is reused by the
  in-process `ASGITransport` (fast unit-of-adapter tests) and, via `MockServer`, by a real
  ephemeral port when a black-box or subprocess worker needs an actual URL. Define once, drive two
  ways.
- **Faithful by default, scriptable on demand** — real happy-path semantics mean idempotency and
  lookup flows are covered without arranging responses; `Override` + state seeding cover the error
  and edge paths. Changing a scenario is a one-liner, never a route rewrite.
- **Real adapter code under test** — auth refresh, `409/404` handling, correlation-id tagging, the
  `Idempotency-Key` header, and JSON shaping all run for real; the seam is a single httpx
  `transport` arg, so production wiring is untouched (`bootstrap.py` passes `None`).
- **Complements, doesn't replace, respx** — the [10-verification](10-verification.md) client checks
  that assert exact wire calls in isolation still use `respx`; the mock servers add stateful,
  multi-call, whole-flow coverage those can't express.
