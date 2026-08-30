"""WorkflowService / WorkflowRunService over the in-memory fakes."""

import logging

import pytest

from orchestrator.domain import (
    ResourceParamsRequired,
    RunStatus,
    RunType,
    TicketRef,
    TicketRefRequired,
    UnknownWorkflowError,
)
from orchestrator.services import WorkflowRunService, WorkflowService
from tests.factories import make_resource_spec, make_workflow

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
    assert run.run_state.workflow.name is None  # unnamed → label falls back to the identifier
    assert run.run_state.workflow.label == "run-automation"
    assert run.run_state.workflow.automation_id == "dag-x"
    assert run.run_state.workflow_params == {"size": "L"}
    assert run.run_state.resource is None
    # The caller's pre-existing RITM was attached — the run will not open its own.
    assert run.run_state.ticket == RITM
    # And it was persisted.
    assert await svc.get(run.run_id) is not None


async def test_trigger_snapshots_the_workflow_name(runs, workflows) -> None:
    # The name is what tickets show; it is snapshotted at trigger time so a later rename does not
    # rewrite the history of runs that already went out.
    await workflows.register(
        make_workflow(identifier="provision-vm", run_type=RunType.RESOURCE, name="Provision VM")
    )
    svc = WorkflowRunService(runs, workflows)

    run = await svc.trigger(
        workflow_identifier="provision-vm",
        created_by="jdoe",
        max_retries=1,
        ticket_params={},
        workflow_params={},
        resource=make_resource_spec(),
        ticket=None,
    )

    assert run.run_state.workflow.name == "Provision VM"
    assert run.run_state.workflow.label == "Provision VM"


async def test_trigger_carries_the_approval_group(runs, workflows) -> None:
    await workflows.register(make_workflow(identifier="new-vm", run_type=RunType.RESOURCE))
    svc = WorkflowRunService(runs, workflows)

    run = await svc.trigger(
        workflow_identifier="new-vm",
        created_by="jdoe",
        max_retries=1,
        ticket_params={},
        workflow_params={},
        resource=make_resource_spec(),
        ticket=None,
        approval_group="CloudIO NetOps",  # the team whose approval the RITM needs
    )

    assert run.run_state.approval_group == "CloudIO NetOps"
    persisted = await svc.get(run.run_id)
    assert persisted is not None and persisted.run_state.approval_group == "CloudIO NetOps"


async def test_trigger_drops_the_approval_group_for_an_automation_run(
    runs, workflows, caplog
) -> None:
    # An automation run attaches to a ticket somebody else opened, so there is nothing to route for
    # approval. Accepted, dropped from the run state, and said out loud rather than ignored quietly.
    await workflows.register(
        make_workflow(identifier="run-automation", run_type=RunType.AUTOMATION)
    )
    svc = WorkflowRunService(runs, workflows)

    with caplog.at_level(logging.WARNING, logger="orchestrator.services"):
        run = await svc.trigger(
            workflow_identifier="run-automation",
            created_by="jdoe",
            max_retries=1,
            ticket_params={},
            workflow_params={},
            resource=None,
            ticket=RITM,
            approval_group="CloudIO NetOps",
        )

    assert run.run_state.approval_group is None
    assert "has no effect" in caplog.text and "CloudIO NetOps" in caplog.text


async def test_trigger_without_an_approval_group_leaves_it_unset(runs, workflows) -> None:
    await workflows.register(make_workflow(identifier="new-vm", run_type=RunType.RESOURCE))
    svc = WorkflowRunService(runs, workflows)

    run = await svc.trigger(
        workflow_identifier="new-vm",
        created_by="jdoe",
        max_retries=1,
        ticket_params={},
        workflow_params={},
        resource=make_resource_spec(),
        ticket=None,
    )

    assert run.run_state.approval_group is None  # routing left to the ticket system


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
