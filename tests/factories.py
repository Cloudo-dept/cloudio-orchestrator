"""Builders for domain entities used across the unit tests."""

from orchestrator.domain import (
    LegacyRequest,
    ResolvedWorkflow,
    ResourceOperation,
    ResourceSpec,
    RunState,
    RunStatus,
    RunType,
    TicketRef,
    Workflow,
    WorkflowEngineType,
    WorkflowRun,
)


def make_workflow(
    *,
    identifier: str = "provision-vm",
    run_type: RunType = RunType.AUTOMATION,
    engine_type: WorkflowEngineType = WorkflowEngineType.AIRFLOW,
    automation_id: str = "dag-x",
    ticket_template_id: str = "cat-1",
    name: str | None = None,
) -> Workflow:
    return Workflow(
        identifier=identifier,
        run_type=run_type,
        engine_type=engine_type,
        automation_id=automation_id,
        ticket_template_id=ticket_template_id,
        name=name,
    )


def make_resource_spec(*, vendor_id: str = "vm-1", resource_id: str = "") -> ResourceSpec:
    return ResourceSpec(
        project_id="proj-1",
        resource_type="vm",
        vendor_id=vendor_id,
        resource_id=resource_id,
        name="app-01",
        region="gvt",
        environment="prod",
    )


def make_legacy_request(
    *,
    flow_type: str = "legacy-provision-vm",
    db_operation: str = "create",
    name: str = "app-01",
    region: str = "gvt",
    project_id: str | None = "proj-1",
    resource_type: str | None = "vm",
) -> LegacyRequest:
    return LegacyRequest(
        flow_type=flow_type,
        project_id=project_id,
        resource_type=resource_type,
        variables={"size": "large"},
        db_operation=db_operation,
        name=name,
        region=region,
    )


def make_run(
    *,
    run_type: RunType = RunType.AUTOMATION,
    created_by: str = "jdoe",
    max_retries: int = 3,
    automation_id: str = "dag-x",
    ticket_template_id: str = "cat-1",
    workflow_name: str | None = None,  # None → a shown label falls back to the identifier
    operation: ResourceOperation = ResourceOperation.CREATE,  # resource runs only
) -> WorkflowRun:
    with_resource = run_type is RunType.RESOURCE
    legacy = run_type is RunType.LEGACY
    state = RunState(
        workflow=ResolvedWorkflow(
            identifier="provision-vm",
            name=workflow_name,
            # A legacy run's engine type names the legacy runner; it has no engine client.
            engine_type=WorkflowEngineType.LEGACY if legacy else WorkflowEngineType.AIRFLOW,
            automation_id=automation_id,
            ticket_template_id="" if legacy else ticket_template_id,
        ),
        ticket_params={} if legacy else {"catalog_variable_1": "value"},
        workflow_params={} if legacy else {"size": "large"},
        resource=make_resource_spec() if with_resource else None,
        operation=operation,
        legacy=make_legacy_request() if legacy else None,
        # Automation runs attach to the caller's pre-existing RITM; resource runs open their own,
        # and legacy runs open none at all (the legacy runner does its own ticketing).
        ticket=(
            None
            if with_resource or legacy
            else TicketRef(ticket_id="RITM0000001", native_id="sys1")
        ),
    )
    return WorkflowRun(
        run_type=run_type,
        status=RunStatus.PENDING,
        workflow_identifier="provision-vm",
        created_by=created_by,
        max_retries=max_retries,
        run_state=state,
    )
