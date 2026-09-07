"""WorkflowService / WorkflowRunService / RunRetryService over the in-memory fakes."""

import uuid

import pytest

from orchestrator.domain import (
    EngineFailure,
    ResourceParamsRequired,
    RunNotFound,
    RunNotRetryable,
    RunStatus,
    RunType,
    StepName,
    TicketRef,
    TicketRefRequired,
    UnknownWorkflowError,
    WorkflowEngineType,
    WorkflowRun,
    utcnow,
)
from orchestrator.orchestration.plans import build_handlers
from orchestrator.services import RunRetryService, WorkflowRunService, WorkflowService
from tests.factories import make_resource_spec, make_run, make_workflow
from tests.fakes import FakeWorkflowRunRepository

RITM = TicketRef(ticket_id="RITM0000001", native_id="sys1")  # a caller-supplied automation ticket


async def test_register_and_get_workflow(workflows) -> None:
    svc = WorkflowService(workflows)
    wf = await svc.register(make_workflow(identifier="provision-vm"))
    assert wf.identifier == "provision-vm"
    fetched = await svc.get("provision-vm")
    assert fetched is not None and fetched.automation_id == "dag-x"
    assert await svc.get("nope") is None
    assert [w.identifier for w in await svc.list()] == ["provision-vm"]


async def test_trigger_builds_run_from_snapshot(runs, workflows) -> None:
    await workflows.register(
        make_workflow(identifier="run-automation", run_type=RunType.AUTOMATION)
    )
    svc = WorkflowRunService(runs, workflows)

    run = await svc.trigger(
        workflow_identifier="run-automation",
        created_by="jdoe",
        max_retries=5,
        ticket_params={"v": 1},
        workflow_params={"size": "L"},
        resource=None,
        ticket=RITM,
    )

    assert run.run_type is RunType.AUTOMATION
    assert run.status is RunStatus.PENDING
    assert run.max_retries == 5
    assert run.scheduled_at is not None  # due now → claimable immediately
    # The registry mapping was snapshotted into the run's state.
    assert run.run_state.workflow.identifier == "run-automation"
    assert run.run_state.workflow.automation_id == "dag-x"
    assert run.run_state.workflow_params == {"size": "L"}
    assert run.run_state.resource is None
    # The caller's pre-existing RITM was attached — the run will not open its own.
    assert run.run_state.ticket == RITM
    # And it was persisted.
    assert await svc.get(run.run_id) is not None


async def test_trigger_resource_workflow_carries_spec(runs, workflows) -> None:
    await workflows.register(make_workflow(identifier="provision-vm", run_type=RunType.RESOURCE))
    svc = WorkflowRunService(runs, workflows)

    run = await svc.trigger(
        workflow_identifier="provision-vm",
        created_by="jdoe",
        max_retries=3,
        ticket_params={},
        workflow_params={},
        resource=make_resource_spec(vendor_id="vm-9"),
        ticket=None,
    )

    assert run.run_type is RunType.RESOURCE
    assert run.run_state.resource is not None
    assert run.run_state.resource.vendor_id == "vm-9"
    assert run.run_state.ticket is None  # resource runs open their own RITM downstream


async def test_trigger_resource_ignores_supplied_ticket(runs, workflows) -> None:
    # A ticket handed to a resource run is dropped — resource runs always create their own RITM.
    await workflows.register(make_workflow(identifier="provision-vm", run_type=RunType.RESOURCE))
    svc = WorkflowRunService(runs, workflows)

    run = await svc.trigger(
        workflow_identifier="provision-vm",
        created_by="jdoe",
        max_retries=3,
        ticket_params={},
        workflow_params={},
        resource=make_resource_spec(vendor_id="vm-9"),
        ticket=RITM,
    )

    assert run.run_state.ticket is None


async def test_trigger_unknown_workflow_raises(runs, workflows) -> None:
    svc = WorkflowRunService(runs, workflows)
    with pytest.raises(UnknownWorkflowError):
        await svc.trigger(
            workflow_identifier="ghost",
            created_by="jdoe",
            max_retries=3,
            ticket_params={},
            workflow_params={},
            resource=None,
            ticket=None,
        )


async def test_trigger_resource_without_spec_raises(runs, workflows) -> None:
    await workflows.register(make_workflow(identifier="provision-vm", run_type=RunType.RESOURCE))
    svc = WorkflowRunService(runs, workflows)
    with pytest.raises(ResourceParamsRequired):
        await svc.trigger(
            workflow_identifier="provision-vm",
            created_by="jdoe",
            max_retries=3,
            ticket_params={},
            workflow_params={},
            resource=None,
            ticket=None,
        )


async def test_trigger_automation_without_ticket_raises(runs, workflows) -> None:
    await workflows.register(
        make_workflow(identifier="run-automation", run_type=RunType.AUTOMATION)
    )
    svc = WorkflowRunService(runs, workflows)
    with pytest.raises(TicketRefRequired):
        await svc.trigger(
            workflow_identifier="run-automation",
            created_by="jdoe",
            max_retries=3,
            ticket_params={},
            workflow_params={},
            resource=None,
            ticket=None,
        )


async def test_find_by_ticket_and_resource(runs, workflows) -> None:
    await workflows.register(make_workflow(identifier="provision-vm", run_type=RunType.RESOURCE))
    svc = WorkflowRunService(runs, workflows)
    run = await svc.trigger(
        workflow_identifier="provision-vm",
        created_by="jdoe",
        max_retries=3,
        ticket_params={},
        workflow_params={},
        resource=make_resource_spec(vendor_id="vm-7"),
        ticket=None,
    )

    found = await svc.find_by_resource_id("vm-7")
    assert [r.run_id for r in found] == [run.run_id]
    assert await svc.find_by_resource_id("absent") == []


# --- RunRetryService: resume a FAILED run where it stopped ---------------------


def retry_service(runs, tickets, resources, engine) -> RunRetryService:
    # The same handler map the executor drives with, so the retry resets step state the same way.
    return RunRetryService(
        runs,
        build_handlers(tickets, resources, {WorkflowEngineType.AIRFLOW: engine}),
        tickets,
    )


async def make_failed_run(
    runs: FakeWorkflowRunRepository, *, step: StepName | None = StepName.RUN_ENGINE
) -> WorkflowRun:
    """A resource run that got as far as RUN_ENGINE and then failed there: the earlier steps'
    idempotency markers are set, and the failed step carries its exhausted bookkeeping."""
    run = make_run(run_type=RunType.RESOURCE)
    run.status, run.current_step, run.scheduled_at = RunStatus.FAILED, step, None
    st = run.run_state
    st.ticket, st.resource_configured = RITM, True  # what the completed steps left behind
    st.engine_run_id = "dagrun-1"
    st.engine_failure = EngineFailure(failed_task="provision_vm", detail="quota exceeded")
    if step is not None:
        st.step_attempts[step] = 4
        st.step_started_at[step] = utcnow()
        st.errors[step] = "Engine run dagrun-1 failed."
    return await runs.create(run)


async def test_retry_resets_the_failed_step_and_makes_the_run_due(
    runs, tickets, resources, engine
) -> None:
    run = await make_failed_run(runs)

    retried = await retry_service(runs, tickets, resources, engine).retry(run.run_id)

    # Re-scheduled, not re-created: a worker claims it now and drives it from current_step.
    assert retried.status is RunStatus.RUNNING
    assert retried.current_step == StepName.RUN_ENGINE
    assert retried.scheduled_at is not None and retried.scheduled_at <= utcnow()
    # The failed step starts clean — attempts, wall-clock deadline, and the recorded error.
    assert StepName.RUN_ENGINE not in retried.run_state.step_attempts
    assert StepName.RUN_ENGINE not in retried.run_state.step_started_at
    assert StepName.RUN_ENGINE not in retried.run_state.errors
    # ...including what its handler considers stale: the failed engine run is not re-attached to.
    assert retried.run_state.engine_run_id is None
    assert retried.run_state.engine_failure is None
    assert retried.run_state.manual_retries == 1
    # ...and it was persisted, not just returned.
    stored = await runs.get(run.run_id)
    assert stored is not None and stored.status is RunStatus.RUNNING


async def test_retry_keeps_what_the_completed_steps_achieved(
    runs, tickets, resources, engine
) -> None:
    # This is what "continue from where it stopped" means: the markers that short-circuit the
    # steps ahead of the failed one survive, so they are not re-run.
    run = await make_failed_run(runs)

    retried = await retry_service(runs, tickets, resources, engine).retry(run.run_id)

    assert retried.run_state.ticket == RITM
    assert retried.run_state.resource_configured is True


async def test_retry_of_a_run_without_a_current_step_restarts_the_plan(
    runs, tickets, resources, engine
) -> None:
    # Defensive: a failed run always names its step, but if it did not, the executor starts the
    # plan from the top — every completed step still short-circuits on its own marker.
    run = await make_failed_run(runs, step=None)

    retried = await retry_service(runs, tickets, resources, engine).retry(run.run_id)

    assert retried.status is RunStatus.RUNNING and retried.current_step is None
    assert retried.run_state.manual_retries == 1


@pytest.mark.parametrize(
    "status", [RunStatus.PENDING, RunStatus.RUNNING, RunStatus.COMPLETED, RunStatus.REJECTED]
)
async def test_retry_is_refused_for_a_run_that_is_not_failed(
    runs, tickets, resources, engine, status
) -> None:
    run = make_run(run_type=RunType.RESOURCE)
    run.status = status
    await runs.create(run)

    with pytest.raises(RunNotRetryable):
        await retry_service(runs, tickets, resources, engine).retry(run.run_id)


async def test_retry_of_an_unknown_run_raises(runs, tickets, resources, engine) -> None:
    with pytest.raises(RunNotFound):
        await retry_service(runs, tickets, resources, engine).retry(uuid.uuid4())


async def test_retry_reopens_the_ticket_of_an_automation_run(
    runs, tickets, resources, engine
) -> None:
    # An automation run's RITM belongs to the caller (ServiceNow opened it), but it is still the
    # record tracking this work, so a retry tells it the work resumed.
    run = make_run(run_type=RunType.AUTOMATION)  # the factory attaches the caller's RITM
    run.status, run.current_step, run.scheduled_at = RunStatus.FAILED, StepName.RUN_ENGINE, None
    await runs.create(run)

    await retry_service(runs, tickets, resources, engine).retry(run.run_id)

    assert [t for t, _ in tickets.reopened] == [run.run_state.ticket.ticket_id]


async def test_retry_survives_a_ticket_system_that_is_down(
    runs, tickets, resources, engine
) -> None:
    # Announcing the retry is best-effort: an operator can still resume a run while ServiceNow
    # is unreachable — the run is what matters, the work note is not.
    class DownTicketClient(type(tickets)):  # type: ignore[misc]
        async def reopen_ticket(self, ticket, note=None):
            raise RuntimeError("ServiceNow unavailable")

    down = DownTicketClient()
    run = await make_failed_run(runs)

    retried = await retry_service(runs, down, resources, engine).retry(run.run_id)

    assert retried.status is RunStatus.RUNNING  # re-scheduled regardless
    assert retried.scheduled_at is not None
