"""Executor + step handlers over the in-memory fakes: happy paths, idempotency, retry, failure."""

import uuid
from typing import Any

import pytest

from orchestrator.config import Settings
from orchestrator.domain import (
    ApprovalStatus,
    EngineFailure,
    EngineRunStatus,
    ResourceOperation,
    RunRejected,
    RunStatus,
    RunType,
    StepName,
    TicketRef,
    WorkflowEngineType,
)
from orchestrator.orchestration.escalator import FailureEscalator
from orchestrator.orchestration.executor import RunExecutor
from orchestrator.orchestration.plans import RUN_PLANS, build_handlers
from orchestrator.orchestration.steps import (
    AwaitApprovalStep,
    CloseTicketStep,
    ConfigureResourceStep,
    CreateTicketStep,
    FinalizeResourceStep,
    StepHandler,
    engine_run_key,
    idem_key,
)
from tests.factories import make_resource_spec, make_run
from tests.fakes import (
    FakeResourceManagerClient,
    FakeTicketSystemClient,
    FakeWorkflowEngineClient,
    FakeWorkflowRunRepository,
)


def build_executor(
    runs: FakeWorkflowRunRepository,
    tickets: FakeTicketSystemClient,
    resources: FakeResourceManagerClient,
    engine: FakeWorkflowEngineClient,
    settings: Settings,
    *,
    incident_team: str = "cloudio",
) -> tuple[RunExecutor, dict[StepName, StepHandler]]:
    handlers = build_handlers(tickets, resources, {WorkflowEngineType.AIRFLOW: engine})
    escalator = FailureEscalator(tickets, incident_team)
    return RunExecutor(handlers, runs, settings, escalator), handlers


async def drive(
    runs: FakeWorkflowRunRepository, executor: RunExecutor, run_id: uuid.UUID, *, iters: int = 12
) -> Any:
    """Stand in for a RunWorker: re-drive the run until it is terminal (or iters run out)."""
    for _ in range(iters):
        run = await runs.get(run_id)
        assert run is not None
        if run.status in (RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.REJECTED):
            return run
        await executor.handle(run_id)
    return await runs.get(run_id)


class FlakyTicketClient(FakeTicketSystemClient):
    """open_ticket raises for the first `fail_times` calls, then behaves normally."""

    def __init__(self, fail_times: int) -> None:
        super().__init__()
        self._remaining = fail_times

    async def open_ticket(
        self, template_id: str, fields: dict[str, Any], requested_by: str, idempotency_key: str
    ) -> TicketRef:
        if self._remaining > 0:
            self._remaining -= 1
            raise RuntimeError("ServiceNow unavailable")
        return await super().open_ticket(template_id, fields, requested_by, idempotency_key)


async def test_automation_run_completes(runs, tickets, resources, engine, settings) -> None:
    executor, _ = build_executor(runs, tickets, resources, engine, settings)
    run = await runs.create(make_run(run_type=RunType.AUTOMATION))

    final = await drive(runs, executor, run.run_id)

    assert final.status is RunStatus.COMPLETED
    assert final.current_step is None
    assert final.scheduled_at is None
    assert len(tickets.open_ticket_calls) == 1  # ticket opened exactly once
    assert len(engine.trigger_calls) == 1  # engine triggered exactly once
    assert tickets.closed and tickets.closed[0][1] == "CloudIO automation completed."
    assert final.run_state.ticket_closed is True


async def test_resource_run_completes_and_finalizes(
    runs, tickets, resources, engine, settings
) -> None:
    executor, _ = build_executor(runs, tickets, resources, engine, settings)
    run = await runs.create(make_run(run_type=RunType.RESOURCE))

    final = await drive(runs, executor, run.run_id)

    assert final.status is RunStatus.COMPLETED
    assert len(resources.create_calls) == 1
    assert final.run_state.resource_configured is True
    assert final.run_state.resource_finalized is True
    # A CREATE assigns the run id as the resource's vendor id when configured...
    assert final.run_state.resource is not None
    assert final.run_state.resource.vendor_id == str(run.run_id)
    # ...and finalize PATCHes in_progress=False on that resource, then closes the RITM.
    assert resources.updated[-1] == ("proj-1", "vm", str(run.run_id), {"in_progress": False})
    assert tickets.closed and tickets.closed[0][1] == "Resource provisioned; request closed."


async def test_engine_polls_until_success(runs, tickets, resources, settings) -> None:
    engine = FakeWorkflowEngineClient(status=EngineRunStatus.IN_PROGRESS)
    executor, _ = build_executor(runs, tickets, resources, engine, settings)
    run = await runs.create(make_run(run_type=RunType.AUTOMATION))

    # While IN_PROGRESS the run never terminates; it keeps re-scheduling a poll.
    mid = await drive(runs, executor, run.run_id, iters=5)
    assert mid.status is RunStatus.RUNNING
    assert mid.current_step == StepName.RUN_ENGINE
    assert mid.scheduled_at is not None
    assert len(engine.trigger_calls) == 1  # triggered once, then only polling

    engine.status = EngineRunStatus.SUCCESS
    final = await drive(runs, executor, run.run_id)
    assert final.status is RunStatus.COMPLETED


async def test_create_ticket_step_is_idempotent(tickets) -> None:
    step = CreateTicketStep(tickets)
    run = make_run(run_type=RunType.AUTOMATION)

    assert await step.execute(run) is True
    first = run.run_state.ticket
    assert first is not None

    assert await step.execute(run) is True  # re-drive: marker short-circuits
    assert run.run_state.ticket == first
    assert len(tickets.open_ticket_calls) == 1  # the provider was hit only once


async def test_transient_failure_retries_then_completes(runs, resources, engine, settings) -> None:
    tickets = FlakyTicketClient(fail_times=2)  # fails twice, then succeeds
    executor, _ = build_executor(runs, tickets, resources, engine, settings)
    run = await runs.create(make_run(run_type=RunType.AUTOMATION, max_retries=3))

    final = await drive(runs, executor, run.run_id, iters=20)

    assert final.status is RunStatus.COMPLETED
    assert final.run_state.step_attempts == {}  # cleared once the step succeeded
    assert len(tickets.open_ticket_calls) == 1  # only the successful call is recorded


async def test_permanent_failure_marks_failed_and_escalates(
    runs, resources, engine, settings
) -> None:
    tickets = FlakyTicketClient(fail_times=99)  # never succeeds
    executor, _ = build_executor(runs, tickets, resources, engine, settings)
    run = await runs.create(make_run(run_type=RunType.AUTOMATION, max_retries=1))

    final = await drive(runs, executor, run.run_id, iters=20)

    assert final.status is RunStatus.FAILED
    assert StepName.CREATE_TICKET in final.run_state.errors
    # Escalation opened an Incident routed to the default team (no engine failure detail).
    assert len(tickets.incidents) == 1
    assert tickets.incidents[0]["responsible_group"] == "cloudio"
    assert final.run_state.incident_id is not None


async def test_engine_failure_escalates_to_responsible_group(
    runs, tickets, resources, settings
) -> None:
    engine = FakeWorkflowEngineClient(status=EngineRunStatus.FAILED)
    engine.failure = EngineFailure(
        failed_task="provision_vm", responsible_group="netops", detail="quota exceeded"
    )
    executor, _ = build_executor(runs, tickets, resources, engine, settings)
    run = await runs.create(make_run(run_type=RunType.AUTOMATION, max_retries=0))

    final = await drive(runs, executor, run.run_id, iters=20)

    assert final.status is RunStatus.FAILED
    assert final.run_state.engine_failure is not None
    assert final.run_state.engine_failure.responsible_group == "netops"
    inc = tickets.incidents[-1]
    assert inc["responsible_group"] == "netops"
    assert inc["failed_task"] == "provision_vm"
    assert inc["flow_type"] == "dag-x"  # automation_id, since a task failed
    # The RITM (opened before the engine step) gets a work note about the incident.
    assert tickets.notes and "Incident" in tickets.notes[-1][1]


async def test_deadline_exceeded_fails_the_step(runs, tickets, resources, engine, settings) -> None:
    executor, handlers = build_executor(runs, tickets, resources, engine, settings)
    handlers[StepName.CREATE_TICKET].max_step_duration_seconds = 0  # any elapsed time trips it
    run = make_run(run_type=RunType.AUTOMATION, max_retries=0)
    # Pretend the step has been in progress since well before now.
    from datetime import timedelta

    from orchestrator.domain import utcnow

    run.run_state.step_started_at[StepName.CREATE_TICKET] = utcnow() - timedelta(seconds=10)
    created = await runs.create(run)

    final = await drive(runs, executor, created.run_id, iters=5)

    assert final.status is RunStatus.FAILED
    assert "budget" in final.run_state.errors[StepName.CREATE_TICKET]
    assert len(tickets.open_ticket_calls) == 0  # never reached the provider call


async def test_terminal_run_is_a_noop(runs, tickets, resources, engine, settings) -> None:
    executor, _ = build_executor(runs, tickets, resources, engine, settings)
    run = await runs.create(make_run(run_type=RunType.AUTOMATION))
    final = await drive(runs, executor, run.run_id)
    calls_before = len(tickets.open_ticket_calls)

    await executor.handle(final.run_id)  # deliver a stale/duplicate wake-up
    assert len(tickets.open_ticket_calls) == calls_before  # nothing happened

    # And an unknown run_id is simply dropped.
    await executor.handle(uuid.uuid4())


async def test_await_approval_step_states(tickets) -> None:
    step = AwaitApprovalStep(tickets)
    run = make_run(run_type=RunType.RESOURCE)
    run.run_state.ticket = TicketRef(ticket_id="RITM0000001", native_id="sys1")

    tickets.approval_status = ApprovalStatus.APPROVED
    assert await step.execute(run) is True

    tickets.approval_status = ApprovalStatus.PENDING
    assert await step.execute(run) is False  # still waiting → poll again later

    tickets.approval_status = ApprovalStatus.REJECTED
    with pytest.raises(RunRejected):
        await step.execute(run)


async def test_resource_run_waits_while_approval_pending(
    runs, tickets, resources, engine, settings
) -> None:
    tickets.approval_status = ApprovalStatus.PENDING
    executor, _ = build_executor(runs, tickets, resources, engine, settings)
    run = await runs.create(make_run(run_type=RunType.RESOURCE))

    mid = await drive(runs, executor, run.run_id, iters=5)

    assert mid.status is RunStatus.RUNNING
    assert mid.current_step == StepName.AWAIT_APPROVAL
    assert mid.scheduled_at is not None  # rescheduled for a later poll, worker released
    assert resources.create_calls == []  # provisioning never started


async def test_resource_run_rejection_terminates_without_incident(
    runs, tickets, resources, engine, settings
) -> None:
    tickets.approval_status = ApprovalStatus.REJECTED
    executor, _ = build_executor(runs, tickets, resources, engine, settings)
    run = await runs.create(make_run(run_type=RunType.RESOURCE))

    final = await drive(runs, executor, run.run_id)

    assert final.status is RunStatus.REJECTED
    assert final.scheduled_at is None
    assert StepName.AWAIT_APPROVAL in final.run_state.errors
    assert tickets.incidents == []  # a rejection is not a failure — no escalation
    assert resources.create_calls == []  # nothing was provisioned


def test_approval_gate_follows_ticket_for_resource_runs() -> None:
    # Resource runs wait for approval right after the ticket; automation runs do not gate.
    assert RUN_PLANS[RunType.RESOURCE][:2] == (StepName.CREATE_TICKET, StepName.AWAIT_APPROVAL)
    assert StepName.AWAIT_APPROVAL not in RUN_PLANS[RunType.AUTOMATION]


def test_finalize_is_two_ordered_steps() -> None:
    # Resource runs finalize the resource, THEN close the ticket; automation runs only close.
    assert RUN_PLANS[RunType.RESOURCE][-2:] == (StepName.FINALIZE_RESOURCE, StepName.CLOSE_TICKET)
    assert RUN_PLANS[RunType.AUTOMATION][-1] == StepName.CLOSE_TICKET
    assert StepName.FINALIZE_RESOURCE not in RUN_PLANS[RunType.AUTOMATION]


async def test_begin_resource_step_create_assigns_run_id_as_vendor_id(resources) -> None:
    step = ConfigureResourceStep(resources)
    run = make_run(run_type=RunType.RESOURCE)  # resource op defaults to CREATE

    assert await step.execute(run) is True
    assert run.run_state.resource_configured is True
    # A CREATE provisions a new record whose vendor id is the run id, in_progress=True.
    assert run.run_state.resource is not None
    assert run.run_state.resource.vendor_id == str(run.run_id)
    assert len(resources.create_calls) == 1
    created = resources.created_by_key[resources.create_calls[0]]
    assert created["vendor_id"] == str(run.run_id)
    assert created["in_progress"] is True
    assert resources.updated == []  # no PATCH — the record was created, not updated

    assert await step.execute(run) is True  # re-drive: marker short-circuits
    assert len(resources.create_calls) == 1


def test_idem_key_is_stable_across_attempts() -> None:
    # A retry must reuse the same key so the provider dedups the side effect (no duplicate).
    run = make_run(run_type=RunType.RESOURCE)
    before = idem_key(run, StepName.CREATE_TICKET)
    run.run_state.step_attempts[StepName.CREATE_TICKET] = 5
    assert idem_key(run, StepName.CREATE_TICKET) == before


def test_engine_run_key_is_fresh_per_attempt() -> None:
    # The engine is the exception: a retry launches a fresh run, so its key is attempt-scoped.
    run = make_run(run_type=RunType.AUTOMATION)
    before = engine_run_key(run)
    run.run_state.step_attempts[StepName.RUN_ENGINE] = 1
    assert engine_run_key(run) != before


async def test_configure_resource_is_idempotent_across_attempts(resources) -> None:
    # The user's example: if the resource already exists in the manager, re-running does NOT
    # create a duplicate — the stable key dedups even when the marker was lost and the attempt
    # advanced (a crash mid-step, then a retry).
    step = ConfigureResourceStep(resources)
    run = make_run(run_type=RunType.RESOURCE)  # CREATE

    assert await step.execute(run) is True
    assert len(resources.created_by_key) == 1

    run.run_state.resource_configured = False  # marker lost before it was persisted
    run.run_state.step_attempts[StepName.CONFIGURE_RESOURCE] = 1  # ...and the step retried
    assert await step.execute(run) is True
    assert len(resources.create_calls) == 2  # the provider was called again...
    assert len(resources.created_by_key) == 1  # ...but deduped to a SINGLE resource


async def test_begin_resource_step_update_only_marks_in_progress(resources) -> None:
    step = ConfigureResourceStep(resources)
    run = make_run(run_type=RunType.RESOURCE)
    run.run_state.resource = make_resource_spec(operation=ResourceOperation.UPDATE)

    assert await step.execute(run) is True
    assert run.run_state.resource_configured is True
    # An UPDATE acts on an existing record — no create, just in_progress=True on the caller's id.
    assert resources.create_calls == []
    assert resources.updated == [("proj-1", "vm", "vm-1", {"in_progress": True})]
    assert run.run_state.resource is not None
    assert run.run_state.resource.vendor_id == "vm-1"  # left untouched


async def test_begin_resource_step_delete_only_marks_in_progress(resources) -> None:
    step = ConfigureResourceStep(resources)
    run = make_run(run_type=RunType.RESOURCE)
    run.run_state.resource = make_resource_spec(operation=ResourceOperation.DELETE)

    assert await step.execute(run) is True
    assert resources.create_calls == []
    assert resources.updated == [("proj-1", "vm", "vm-1", {"in_progress": True})]


async def test_finalize_resource_step(resources) -> None:
    step = FinalizeResourceStep(resources)

    # No resource (automation) → no-op, no provider call.
    assert await step.execute(make_run(run_type=RunType.AUTOMATION)) is True
    assert resources.updated == []

    # Resource run → PATCHes in_progress=False once, idempotent on the marker.
    run = make_run(run_type=RunType.RESOURCE)
    assert await step.execute(run) is True
    assert run.run_state.resource_finalized is True
    assert resources.updated == [("proj-1", "vm", "vm-1", {"in_progress": False})]
    assert await step.execute(run) is True  # re-drive: marker short-circuits
    assert len(resources.updated) == 1


async def test_close_ticket_step(tickets) -> None:
    step = CloseTicketStep(tickets)
    run = make_run(run_type=RunType.RESOURCE)
    run.run_state.ticket = TicketRef(ticket_id="RITM0000001", native_id="sys1")

    assert await step.execute(run) is True
    assert run.run_state.ticket_closed is True
    assert tickets.closed == [("RITM0000001", "Resource provisioned; request closed.")]
    assert await step.execute(run) is True  # re-drive: marker short-circuits
    assert len(tickets.closed) == 1


async def test_engine_final_vendor_id_overrides_finalize_target(
    runs, tickets, resources, settings
) -> None:
    engine = FakeWorkflowEngineClient()  # SUCCESS
    engine.outputs["final_vendor_id"] = "vm-engine-99"  # the run reports the provisioned id
    executor, _ = build_executor(runs, tickets, resources, engine, settings)
    run = await runs.create(make_run(run_type=RunType.RESOURCE))

    final = await drive(runs, executor, run.run_id)

    assert final.status is RunStatus.COMPLETED
    # The engine-reported id lands under data.vendor_id. Finalize PATCHes the record where it was
    # created (the run id) and re-keys it to the engine-reported id as it marks it done.
    assert final.run_state.resource is not None
    assert final.run_state.resource.data["vendor_id"] == "vm-engine-99"
    assert resources.updated[-1] == (
        "proj-1",
        "vm",
        str(run.run_id),
        {"in_progress": False, "vendor_id": "vm-engine-99"},
    )


async def test_finalize_falls_back_to_original_vendor_id(
    runs, tickets, resources, engine, settings
) -> None:
    # The engine (default fake) publishes no output → finalize keeps the begin-time vendor_id
    # (the run id, since this is a CREATE).
    executor, _ = build_executor(runs, tickets, resources, engine, settings)
    run = await runs.create(make_run(run_type=RunType.RESOURCE))

    final = await drive(runs, executor, run.run_id)

    assert resources.updated[-1] == ("proj-1", "vm", str(run.run_id), {"in_progress": False})
    assert final.run_state.resource is not None
    assert "vendor_id" not in final.run_state.resource.data
