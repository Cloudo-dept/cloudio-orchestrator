*[← Index](README.md)*

# Entrypoints — services, API, worker, CLI, bootstrap

Two processes, one composition root. The **API** writes run state (with `scheduled_at=now`); the
**worker daemon** runs a pool of `RunWorker` loops, each of which claims one due run and drives it.
Everything is constructor-injected; `bootstrap.build()` is the only place adapters are bound to
ports.

## `services.py` — use-cases the API delegates to

```python
import uuid
from typing import Any

from orchestrator.domain import (ResolvedWorkflow, ResourceParamsRequired, ResourceSpec,
                                 RunState, RunStatus, RunType, UnknownWorkflowError,
                                 Workflow, WorkflowRun)
from orchestrator.ports import WorkflowRepository, WorkflowRunRepository


class WorkflowService:
    def __init__(self, workflows: WorkflowRepository) -> None:
        self.workflows = workflows

    async def register(self, workflow: Workflow) -> Workflow:
        return await self.workflows.register(workflow)

    async def get(self, identifier: str) -> Workflow | None:
        return await self.workflows.get_by_identifier(identifier)

    async def list(self) -> list[Workflow]:
        return await self.workflows.list()


class WorkflowRunService:
    def __init__(self, runs: WorkflowRunRepository, workflows: WorkflowRepository) -> None:
        self.runs = runs
        self.workflows = workflows

    async def trigger(self, *, workflow_identifier: str, created_by: str, max_retries: int,
                      ticket_params: dict[str, Any], workflow_params: dict[str, Any],
                      resource: ResourceSpec | None) -> WorkflowRun:
        wf = await self.workflows.get_by_identifier(workflow_identifier)
        if wf is None:
            raise UnknownWorkflowError(workflow_identifier)
        if wf.run_type is RunType.RESOURCE and resource is None:
            raise ResourceParamsRequired(workflow_identifier)

        state = RunState(
            workflow=ResolvedWorkflow(
                identifier=wf.identifier, engine_type=wf.engine_type,
                automation_id=wf.automation_id, ticket_template_id=wf.ticket_template_id),
            ticket_params=ticket_params,
            workflow_params=workflow_params,
            resource=resource if wf.run_type is RunType.RESOURCE else None,
        )
        # scheduled_at defaults to now → a RunWorker claims it on its next scan.
        run = WorkflowRun(run_type=wf.run_type, status=RunStatus.PENDING,
                          workflow_identifier=wf.identifier, created_by=created_by,
                          max_retries=max_retries, run_state=state)
        return await self.runs.create(run)

    async def get(self, run_id: uuid.UUID) -> WorkflowRun | None:
        return await self.runs.get(run_id)

    async def find_by_ticket_id(self, ticket_id: str) -> list[WorkflowRun]:
        return await self.runs.find_by_ticket_id(ticket_id)

    async def find_by_resource_id(self, vendor_id: str) -> list[WorkflowRun]:
        return await self.runs.find_by_resource_id(vendor_id)
```

## `api.py` — FastAPI app + HTTP schemas

Explicit request/response DTOs keep the HTTP contract decoupled from the persistence shape.
Completion is polled; a **callbacks router** (`POST /api/v1/callbacks/ticket-approval`,
`POST /api/v1/callbacks/engine-run`) co-exists as a wake-early optimization — an external
notification only makes the waiting run due now so it re-polls immediately (see
[01-external-contracts](01-external-contracts.md)).

```python
import uuid
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Request, status
from pydantic import BaseModel, ConfigDict, Field

from orchestrator.domain import (ResourceParamsRequired, ResourceSpec, RunState, RunStatus,
                                 RunType, UnknownWorkflowError, Workflow, WorkflowEngineType)
from orchestrator.services import WorkflowRunService, WorkflowService


# --- HTTP schemas ---

class WorkflowRegisterRequest(BaseModel):
    identifier: str = Field(..., max_length=255)      # stable key callers trigger with
    run_type: RunType
    engine_type: WorkflowEngineType
    automation_id: str                                # Airflow: the DAG id
    ticket_template_id: str                           # ServiceNow: catalog item sys_id
    name: str | None = None


class WorkflowResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    workflow_id: uuid.UUID
    identifier: str
    name: str | None
    run_type: RunType
    engine_type: WorkflowEngineType
    automation_id: str
    ticket_template_id: str


class WorkflowRunTriggerRequest(BaseModel):
    """Caller names a registered workflow and supplies the parameter sets; the orchestrator
    resolves the registration to decide the run type and how to build it."""
    workflow_identifier: str
    created_by: str = Field(..., max_length=255)
    max_retries: int = Field(3, ge=0, le=10)
    ticket_params: dict[str, Any] = Field(default_factory=dict)     # provider template variables
    workflow_params: dict[str, Any] = Field(default_factory=dict)   # engine conf
    resource: ResourceSpec | None = None                            # resource workflows only
    ticket: TicketRef | None = None                                 # existing RITM (automation only)


class WorkflowRunResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    run_id: uuid.UUID
    run_type: RunType
    status: RunStatus
    current_step: str | None
    created_by: str
    max_retries: int
    run_state: RunState                               # typed — clients see a real schema


# --- App + routes (dependencies wired by bootstrap via app.state.container) ---

app = FastAPI(title="Orchestrator Core API")


def get_workflow_service(request: Request) -> WorkflowService:
    return request.app.state.container.workflow_service


def get_run_service(request: Request) -> WorkflowRunService:
    return request.app.state.container.run_service


@app.post("/api/v1/workflows", response_model=WorkflowResponse,
          status_code=status.HTTP_201_CREATED)
async def register_workflow(request: WorkflowRegisterRequest,
                            svc: WorkflowService = Depends(get_workflow_service)):
    return await svc.register(Workflow(
        identifier=request.identifier, name=request.name, run_type=request.run_type,
        engine_type=request.engine_type, automation_id=request.automation_id,
        ticket_template_id=request.ticket_template_id))


@app.get("/api/v1/workflows/{identifier}", response_model=WorkflowResponse)
async def get_workflow(identifier: str,
                       svc: WorkflowService = Depends(get_workflow_service)):
    wf = await svc.get(identifier)
    if wf is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail=f"Workflow '{identifier}' not found.")
    return wf


@app.post("/api/v1/workflow-runs", response_model=WorkflowRunResponse,
          status_code=status.HTTP_201_CREATED)
async def trigger_workflow_run(request: WorkflowRunTriggerRequest,
                               svc: WorkflowRunService = Depends(get_run_service)):
    try:
        return await svc.trigger(
            workflow_identifier=request.workflow_identifier, created_by=request.created_by,
            max_retries=request.max_retries, ticket_params=request.ticket_params,
            workflow_params=request.workflow_params, resource=request.resource)
    except UnknownWorkflowError:
        raise HTTPException(status.HTTP_404_NOT_FOUND,
                            detail=f"Unknown workflow '{request.workflow_identifier}'.")
    except ResourceParamsRequired:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail="resource is required for a resource workflow.")


@app.get("/api/v1/workflow-runs/{run_id}", response_model=WorkflowRunResponse)
async def get_workflow_run(run_id: uuid.UUID,
                           svc: WorkflowRunService = Depends(get_run_service)):
    run = await svc.get(run_id)
    if run is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail=f"Run {run_id} not found.")
    return run


@app.get("/api/v1/workflow-runs", response_model=list[WorkflowRunResponse])
async def list_workflow_runs(ticket_id: str | None = None, resource_id: str | None = None,
                             svc: WorkflowRunService = Depends(get_run_service)):
    if ticket_id:
        return await svc.find_by_ticket_id(ticket_id)
    if resource_id:
        return await svc.find_by_resource_id(resource_id)
    raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="Provide ticket_id or resource_id.")
```

## `worker.py` — daemon: workers claim and drive runs directly

The daemon runs a pool of identical **`RunWorker`** loops (`worker_concurrency_limit` of them).
Each loop claims **one** due run (`claim_due(1)`, `scheduled_at <= now`, `FOR UPDATE SKIP LOCKED`),
drives it through `RunExecutor.handle`, and claims again — no scheduler, no queue, no message.
`claim_due` pushes the claimed run's `scheduled_at` forward by a lease, so a run whose processing
never completes (crash) becomes due again and is re-driven by any worker — crash recovery with
nothing but the run store. A worker only claims when it's free, so a claimed run is always being
actively worked (no lease burned in a buffer). `SKIP LOCKED` keeps the loops — and multiple
daemons — from ever claiming the same run.

```python
import asyncio
import logging

from orchestrator.orchestration.executor import RunExecutor
from orchestrator.ports import WorkflowRunRepository

logger = logging.getLogger(__name__)


class RunWorker:
    """One claim-and-drive loop: pull a single due run (FOR UPDATE SKIP LOCKED), drive it,
    repeat. The re-drive lease on claim_due (not a transport) is what makes a crash re-drive,
    so no queue and no scheduler are needed. Identical in every worker and every daemon."""

    def __init__(self, runs: WorkflowRunRepository, executor: RunExecutor, *,
                 poll_interval_seconds: float, lease_seconds: float) -> None:
        self.runs = runs
        self.executor = executor
        self.poll_interval = poll_interval_seconds
        self.lease_seconds = lease_seconds
        self._stop = asyncio.Event()

    def request_stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        while not self._stop.is_set():
            run_ids = await self.runs.claim_due(1, self.lease_seconds)   # 0 or 1 due run
            if not run_ids:                                             # nothing due → wait a tick
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=self.poll_interval)
                except TimeoutError:
                    pass
                continue
            try:
                await self.executor.handle(run_ids[0])
            except Exception:
                logger.exception("Run %s crashed mid-drive; the lease will re-drive it.",
                                 run_ids[0])


class OrchestratorWorker:
    """The daemon: runs `concurrency_limit` identical RunWorker loops concurrently. Each claims
    its own work via SKIP LOCKED, so the loops — and multiple daemons — never collide."""

    def __init__(self, runs: WorkflowRunRepository, executor: RunExecutor, *,
                 concurrency_limit: int, poll_interval_seconds: float,
                 lease_seconds: float) -> None:
        self._workers = [
            RunWorker(runs, executor, poll_interval_seconds=poll_interval_seconds,
                      lease_seconds=lease_seconds)
            for _ in range(concurrency_limit)
        ]

    def request_stop(self) -> None:
        for worker in self._workers:
            worker.request_stop()

    async def start(self) -> None:
        logger.info("Worker started; %d loops claiming + driving runs.", len(self._workers))
        await asyncio.gather(*(worker.run() for worker in self._workers))
        logger.info("Worker stopped.")
```

## `bootstrap.py` — composition root (the one place wiring happens)

```python
from pydantic import BaseModel, ConfigDict

from orchestrator.adapters.airflow import AirflowWorkflowEngineClient
from orchestrator.adapters.database import (PostgresWorkflowRepository,
                                            PostgresWorkflowRunRepository,
                                            make_session_factory)
from orchestrator.adapters.project_manager import ProjectManagerResourceClient
from orchestrator.adapters.servicenow import ServiceNowTicketClient
from orchestrator.config import Settings
from orchestrator.domain import WorkflowEngineType
from orchestrator.orchestration.escalator import FailureEscalator
from orchestrator.orchestration.executor import RunExecutor
from orchestrator.orchestration.plans import build_handlers
from orchestrator.services import WorkflowRunService, WorkflowService
from orchestrator.worker import OrchestratorWorker


class Container(BaseModel):
    """Everything wired; entrypoints pick what they need (API: services; worker: worker)."""
    model_config = ConfigDict(arbitrary_types_allowed=True)

    workflow_service: WorkflowService
    run_service: WorkflowRunService
    worker: OrchestratorWorker


async def build(settings: Settings) -> Container:
    session_factory = make_session_factory(settings.database_url)
    runs = PostgresWorkflowRunRepository(session_factory)
    workflows = PostgresWorkflowRepository(session_factory)

    ticket_client = ServiceNowTicketClient(
        settings.servicenow_base_url, settings.servicenow_username,
        settings.servicenow_password.get_secret_value(),
        settings.servicenow_responsible_groups, settings.external_call_timeout_seconds)
    resource_client = ProjectManagerResourceClient(
        settings.pm_base_url, settings.pm_token.get_secret_value(),
        settings.external_call_timeout_seconds)
    engines = {
        WorkflowEngineType.AIRFLOW: AirflowWorkflowEngineClient(
            settings.airflow_base_url, settings.airflow_username,
            settings.airflow_password.get_secret_value(),
            settings.external_call_timeout_seconds),
    }

    handlers = build_handlers(ticket_client, resource_client, engines)
    escalator = FailureEscalator(ticket_client, settings.servicenow_incident_team)
    executor = RunExecutor(handlers, runs, settings, escalator)   # sets scheduled_at for polls/retries
    worker = OrchestratorWorker(runs, executor,
                                concurrency_limit=settings.worker_concurrency_limit,
                                poll_interval_seconds=settings.worker_poll_interval_seconds,
                                lease_seconds=settings.redrive_lease_seconds)

    return Container(
        workflow_service=WorkflowService(workflows),
        run_service=WorkflowRunService(runs, workflows),
        worker=worker)
```

## `cli.py` — one installed script, two commands

```python
import asyncio

import typer
import uvicorn

from orchestrator.bootstrap import build
from orchestrator.config import Settings

app = typer.Typer(help="CloudIO orchestrator")


@app.command()
def api(host: str = "0.0.0.0", port: int = 8000) -> None:
    """Serve the HTTP API."""
    uvicorn.run("orchestrator.api:app", host=host, port=port, factory=False)


@app.command()
def worker() -> None:
    """Run the daemon (claims due runs and drives them)."""
    async def _main() -> None:
        container = await build(Settings())
        await container.worker.start()

    asyncio.run(_main())
```

*(The API process attaches the container in a lifespan hook: `app.state.container = await
build(Settings())`.)*

## Example — register once, then trigger

```json
POST /api/v1/workflows
{
  "identifier": "provision-vm",
  "name": "Provision a VM",
  "run_type": "resource",
  "engine_type": "airflow",
  "automation_id": "provision_vm_dag",
  "ticket_template_id": "a1b2c3d4e5f6..."
}
```

```json
POST /api/v1/workflow-runs
{
  "workflow_identifier": "provision-vm",
  "created_by": "jdoe",
  "ticket_params": {"catalog_variable_1": "value"},
  "workflow_params": {"size": "large"},
  "resource": {
    "project_id": "proj-123", "resource_type": "vm", "vendor_id": "vm-abc-01",
    "name": "app-server-01", "region": "gvt", "environment": "prod",
    "description": "App server", "tags": ["cloudio"]
  }
}
```

The orchestrator resolves `provision-vm` → `run_type=resource`, `engine=airflow`,
`automation_id=provision_vm_dag`, catalog item to order — and builds the run. `created_by` is the
ServiceNow `caller_id`/`sysparm_requested_for` and the resource `last_modified_by`.
`ticket_params` are the catalog-item variables; `resource` is a typed `ResourceSpec` (validated
at the HTTP boundary, not deep inside a step).

An `automation` workflow omits `resource` and instead supplies `ticket` — a `TicketRef`
(`{ticket_id, native_id}`) for the RITM the caller already created. ServiceNow triggers these runs:
a user opens an RITM, and a ServiceNow outbound REST action POSTs here with that RITM. The run
**attaches** to it (there is no CREATE_TICKET step for automation) and closes it when the engine
finishes. Triggering an automation workflow without a `ticket` is a `422`.

## What got simpler here (vs. the previous revision)

- **The callbacks router is a pure wake-early nudge, not a status writer** — it sets
  `scheduled_at=now` via `runs.wake(...)` and lets the existing poll step read the authoritative
  status; no `record_callback`, no provider status trusted from the body (see
  [01-external-contracts](01-external-contracts.md)). Auth is network-trust (no HMAC/token).
- **`api.py` is one module** (schemas + routes) instead of `app.py` + `dependencies.py` +
  `schemas/` + `routers/` — six files for five routes was ceremony.
- **`resource` is a typed `ResourceSpec` in the request** — validation errors surface as 422s at
  the boundary instead of `KeyError`s inside `ConfigureResourceStep`.
- **Typed domain errors** (`UnknownWorkflowError`, `ResourceParamsRequired`) map to HTTP codes in
  the router — services stay HTTP-free.
- **`cli.py` (typer)** replaces ad-hoc `python -m` invocations: `orchestrator api`,
  `orchestrator worker`.
