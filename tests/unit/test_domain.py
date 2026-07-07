"""Domain models: enum values, RunState JSONB round-trip, validation, and entity defaults."""

import uuid
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from orchestrator.domain import (
    EngineRunStatus,
    PydanticJSONB,
    ResolvedWorkflow,
    ResourceSpec,
    RunState,
    RunStatus,
    RunType,
    StepName,
    TicketRef,
    Workflow,
    WorkflowEngineType,
    WorkflowRun,
    _enum_values,
)


def make_run_state(*, with_resource: bool = False) -> RunState:
    return RunState(
        workflow=ResolvedWorkflow(
            identifier="provision-vm",
            engine_type=WorkflowEngineType.AIRFLOW,
            automation_id="dag-x",
            ticket_template_id="cat-1",
        ),
        ticket_params={"catalog_variable_1": "value"},
        workflow_params={"size": "large"},
        resource=(
            ResourceSpec(
                project_id="proj-1",
                resource_type="vm",
                vendor_id="vm-1",
                name="app-01",
                region="gvt",
                environment="prod",
            )
            if with_resource
            else None
        ),
    )


def test_enum_values_are_lowercase_strings() -> None:
    assert _enum_values(RunType) == ["automation", "resource"]
    assert _enum_values(RunStatus) == ["pending", "running", "completed", "failed", "rejected"]
    assert _enum_values(WorkflowEngineType) == ["airflow"]
    # str-enum comparison against a raw string works (used for current_step column).
    assert StepName.CREATE_TICKET == "creating_ticket"
    assert EngineRunStatus.IN_PROGRESS.value == "in_progress"


def test_pydantic_jsonb_roundtrips_runstate() -> None:
    decorator = PydanticJSONB(RunState)
    state = make_run_state(with_resource=True)
    state.ticket = TicketRef(ticket_id="RITM0000001", native_id="sys1")
    state.step_attempts[StepName.RUN_ENGINE] = 2
    state.step_started_at[StepName.RUN_ENGINE] = datetime(2026, 7, 6, 12, 0, tzinfo=UTC)

    dumped = decorator.process_bind_param(state, dialect=object())
    assert isinstance(dumped, dict)
    # StepName keys serialize to their lowercase string values in JSON.
    assert dumped["step_attempts"] == {"running_engine": 2}

    loaded = decorator.process_result_value(dumped, dialect=object())
    assert isinstance(loaded, RunState)
    assert loaded == state
    # ... and the StepName key is reconstructed as the enum, not a bare string.
    assert loaded.step_attempts[StepName.RUN_ENGINE] == 2


def test_pydantic_jsonb_handles_none() -> None:
    decorator = PydanticJSONB(RunState)
    assert decorator.process_bind_param(None, dialect=object()) is None
    assert decorator.process_result_value(None, dialect=object()) is None


def test_runstate_validation_fails_loudly_on_bad_shape() -> None:
    # Missing the required nested `workflow` mapping -> ValidationError, not a silent empty model.
    with pytest.raises(ValidationError):
        RunState.model_validate({"workflow_params": {"size": "L"}})
    # Wrong type for a typed field.
    with pytest.raises(ValidationError):
        RunState.model_validate(
            {
                "workflow": {
                    "identifier": "x",
                    "engine_type": "airflow",
                    "automation_id": "d",
                    "ticket_template_id": "c",
                },
                "resource_configured": "not-a-bool-ish",
            }
        )


def test_workflow_run_defaults() -> None:
    run = WorkflowRun(
        run_type=RunType.RESOURCE,
        workflow_identifier="provision-vm",
        created_by="jdoe",
        run_state=make_run_state(with_resource=True),
    )
    assert isinstance(run.run_id, uuid.UUID)
    assert run.status is RunStatus.PENDING
    assert run.version == 1
    assert run.max_retries == 3
    assert run.current_step is None
    assert run.scheduled_at is not None and run.scheduled_at.tzinfo is not None


def test_workflow_defaults() -> None:
    wf = Workflow(
        identifier="provision-vm",
        run_type=RunType.RESOURCE,
        engine_type=WorkflowEngineType.AIRFLOW,
        automation_id="dag-x",
        ticket_template_id="cat-1",
    )
    assert isinstance(wf.workflow_id, uuid.UUID)
    assert wf.name is None
    assert wf.created_at.tzinfo is not None
