"""FastAPI app + HTTP schemas + routers.

Explicit request/response DTOs keep the HTTP contract decoupled from the persistence shape.
There is no callbacks router — completion is polled.
"""

import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Request, status
from pydantic import BaseModel, ConfigDict, Field

from orchestrator.domain import (
    ResourceParamsRequired,
    ResourceSpec,
    RunState,
    RunStatus,
    RunType,
    UnknownWorkflowError,
    Workflow,
    WorkflowAlreadyExistsError,
    WorkflowEngineType,
)
from orchestrator.ports import HealthCheck
from orchestrator.services import WorkflowRunService, WorkflowService

# --- HTTP schemas ---


class WorkflowRegisterRequest(BaseModel):
    identifier: str = Field(..., max_length=255)  # stable key callers trigger with
    run_type: RunType
    engine_type: WorkflowEngineType
    automation_id: str  # Airflow: the DAG id
    ticket_template_id: str  # ServiceNow: catalog item sys_id
    name: str | None = None


class WorkflowUpdateRequest(BaseModel):
    """Editable registration fields; identifier is immutable and comes from the path."""

    run_type: RunType
    engine_type: WorkflowEngineType
    automation_id: str
    ticket_template_id: str
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
    ticket_params: dict[str, Any] = Field(default_factory=dict)  # provider template variables
    workflow_params: dict[str, Any] = Field(default_factory=dict)  # engine conf
    resource: ResourceSpec | None = None  # resource workflows only


class WorkflowRunResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    run_id: uuid.UUID
    run_type: RunType
    status: RunStatus
    current_step: str | None
    created_by: str
    max_retries: int
    run_state: RunState  # typed — clients see a real schema


class HealthResponse(BaseModel):
    status: str  # "ok" (liveness) / "ready" | "unavailable" (readiness)


# --- App + routes (dependencies wired by bootstrap via app.state.container) ---


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    # Built lazily so importing this module (e.g. in tests) does not require a database.
    from loguru import logger

    from orchestrator.bootstrap import build
    from orchestrator.config import Settings
    from orchestrator.log import configure_logging

    settings = Settings()
    configure_logging(settings.log_level)
    logger.info("API lifespan startup: building container.")
    app.state.container = await build(settings)
    logger.info("API ready to serve requests.")
    yield
    logger.info("API lifespan shutdown.")


app = FastAPI(title="Orchestrator Core API", lifespan=lifespan)


def get_workflow_service(request: Request) -> WorkflowService:
    service: WorkflowService = request.app.state.container.workflow_service
    return service


def get_run_service(request: Request) -> WorkflowRunService:
    service: WorkflowRunService = request.app.state.container.run_service
    return service


def get_health_check(request: Request) -> HealthCheck:
    health_check: HealthCheck = request.app.state.container.health_check
    return health_check


@app.get("/healthz", response_model=HealthResponse, tags=["health"])
async def healthz() -> HealthResponse:
    """Liveness: the process is up and serving. Does not touch the database."""
    return HealthResponse(status="ok")


@app.get("/readyz", response_model=HealthResponse, tags=["health"])
async def readyz(health_check: HealthCheck = Depends(get_health_check)) -> HealthResponse:
    """Readiness: the run store is reachable. 503 (not raising) if the probe fails, so
    orchestrators drain traffic instead of killing the pod."""
    try:
        await health_check.ping()
    except Exception:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE, detail="run store unreachable"
        ) from None
    return HealthResponse(status="ready")


@app.post("/api/v1/workflows", response_model=WorkflowResponse, status_code=status.HTTP_201_CREATED)
async def register_workflow(
    request: WorkflowRegisterRequest,
    svc: WorkflowService = Depends(get_workflow_service),
) -> Workflow:
    try:
        return await svc.register(
            Workflow(
                identifier=request.identifier,
                name=request.name,
                run_type=request.run_type,
                engine_type=request.engine_type,
                automation_id=request.automation_id,
                ticket_template_id=request.ticket_template_id,
            )
        )
    except WorkflowAlreadyExistsError:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            detail=f"Workflow '{request.identifier}' is already registered.",
        ) from None


@app.get("/api/v1/workflows", response_model=list[WorkflowResponse])
async def list_workflows(
    svc: WorkflowService = Depends(get_workflow_service),
) -> list[Workflow]:
    return await svc.list()


@app.get("/api/v1/workflows/{identifier}", response_model=WorkflowResponse)
async def get_workflow(
    identifier: str,
    svc: WorkflowService = Depends(get_workflow_service),
) -> Workflow:
    wf = await svc.get(identifier)
    if wf is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail=f"Workflow '{identifier}' not found.")
    return wf


@app.put("/api/v1/workflows/{identifier}", response_model=WorkflowResponse)
async def update_workflow(
    identifier: str,
    request: WorkflowUpdateRequest,
    svc: WorkflowService = Depends(get_workflow_service),
) -> Workflow:
    updated = await svc.update(
        Workflow(
            identifier=identifier,
            name=request.name,
            run_type=request.run_type,
            engine_type=request.engine_type,
            automation_id=request.automation_id,
            ticket_template_id=request.ticket_template_id,
        )
    )
    if updated is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail=f"Workflow '{identifier}' not found.")
    return updated


@app.post(
    "/api/v1/workflow-runs", response_model=WorkflowRunResponse, status_code=status.HTTP_201_CREATED
)
async def trigger_workflow_run(
    request: WorkflowRunTriggerRequest,
    svc: WorkflowRunService = Depends(get_run_service),
) -> Any:
    try:
        return await svc.trigger(
            workflow_identifier=request.workflow_identifier,
            created_by=request.created_by,
            max_retries=request.max_retries,
            ticket_params=request.ticket_params,
            workflow_params=request.workflow_params,
            resource=request.resource,
        )
    except UnknownWorkflowError:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND, detail=f"Unknown workflow '{request.workflow_identifier}'."
        ) from None
    except ResourceParamsRequired:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="resource is required for a resource workflow.",
        ) from None


@app.get("/api/v1/workflow-runs/{run_id}", response_model=WorkflowRunResponse)
async def get_workflow_run(
    run_id: uuid.UUID,
    svc: WorkflowRunService = Depends(get_run_service),
) -> Any:
    run = await svc.get(run_id)
    if run is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail=f"Run {run_id} not found.")
    return run


@app.get("/api/v1/workflow-runs", response_model=list[WorkflowRunResponse])
async def list_workflow_runs(
    ticket_id: str | None = None,
    resource_id: str | None = None,
    svc: WorkflowRunService = Depends(get_run_service),
) -> Any:
    if ticket_id:
        return await svc.find_by_ticket_id(ticket_id)
    if resource_id:
        return await svc.find_by_resource_id(resource_id)
    return await svc.list_recent()  # unfiltered: the most recent runs, newest first
