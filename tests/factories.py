"""Builders for domain entities used across the unit tests."""

from orchestrator.domain import (
    ResolvedWorkflow,
    ResourceOperation,
    ResourceSpec,
    RunState,
    RunStatus,
    RunType,
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


def make_resource_spec(
    *,
    vendor_id: str = "vm-1",
    operation: ResourceOperation = ResourceOperation.CREATE,
) -> ResourceSpec:
    return ResourceSpec(
        project_id="proj-1",
        resource_type="vm",
        operation=operation,
        vendor_id=vendor_id,
        name="app-01",
        region="gvt",
        environment="prod",
    )


def make_run(
    *,
    run_type: RunType = RunType.AUTOMATION,
    created_by: str = "jdoe",
    max_retries: int = 3,
    automation_id: str = "dag-x",
    ticket_template_id: str = "cat-1",
) -> WorkflowRun:
    with_resource = run_type is RunType.RESOURCE
    state = RunState(
        workflow=ResolvedWorkflow(
            identifier="provision-vm",
            engine_type=WorkflowEngineType.AIRFLOW,
            automation_id=automation_id,
            ticket_template_id=ticket_template_id,
        ),
        ticket_params={"catalog_variable_1": "value"},
        workflow_params={"size": "large"},
        resource=make_resource_spec() if with_resource else None,
    )
    return WorkflowRun(
        run_type=run_type,
        status=RunStatus.PENDING,
        workflow_identifier="provision-vm",
        created_by=created_by,
        max_retries=max_retries,
        run_state=state,
    )
