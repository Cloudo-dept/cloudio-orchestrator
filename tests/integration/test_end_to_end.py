"""Full run over real Postgres + the three provider mocks, driven the way a RunWorker would.

Docker-gated (testcontainers Postgres).
"""

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker

from orchestrator.adapters.airflow import AirflowWorkflowEngineClient
from orchestrator.adapters.database import (
    PostgresWorkflowRepository,
    PostgresWorkflowRunRepository,
)
from orchestrator.adapters.project_manager import ProjectManagerResourceClient
from orchestrator.adapters.servicenow import ServiceNowTicketClient
from orchestrator.config import Settings
from orchestrator.domain import (
    ResourceOperation,
    RunStatus,
    RunType,
    StepName,
    TicketRef,
    WorkflowEngineType,
)
from orchestrator.orchestration.escalator import FailureEscalator
from orchestrator.orchestration.executor import RunExecutor
from orchestrator.orchestration.plans import build_handlers
from orchestrator.services import WorkflowRunService
from tests.factories import make_resource_spec, make_workflow
from tests.mocks.airflow import AirflowMock
from tests.mocks.base import mock_client
from tests.mocks.project_manager import ProjectManagerMock
from tests.mocks.servicenow import ServiceNowMock

pytestmark = pytest.mark.integration


async def _assemble(pg_session_factory, servicenow, airflow, project_manager):
    # The clients are not closed: an ASGI transport holds no sockets, and these live for the
    # duration of one test. Production lifecycle is the container's job (bootstrap.Container).
    tickets = ServiceNowTicketClient(
        mock_client(servicenow.app, "http://sn.local", auth=("u", "p")),
        responsible_groups={"netops": "grpsys-netops"},
        default_group="cloudio",
        group_lookup_field=servicenow.group_lookup_field,
        user_lookup_field=servicenow.user_lookup_field,
    )
    engine = AirflowWorkflowEngineClient(
        mock_client(airflow.app, "http://af.local"), username="u", password="p"
    )
    resources = ProjectManagerResourceClient(
        mock_client(project_manager.app, "http://pm.local", headers={"Authorization": "Bearer t"})
    )
    runs = PostgresWorkflowRunRepository(pg_session_factory)
    workflows = PostgresWorkflowRepository(pg_session_factory)
    handlers = build_handlers(tickets, resources, {WorkflowEngineType.AIRFLOW: engine})
    handlers[StepName.RUN_ENGINE].poll_interval_seconds = 0  # re-drive immediately
    handlers[StepName.AWAIT_APPROVAL].poll_interval_seconds = 0  # re-drive immediately
    executor = RunExecutor(
        handlers,
        runs,
        Settings.model_construct(retry_base_seconds=1.0),
        FailureEscalator(tickets, resources, "cloudio"),
    )
    return runs, WorkflowRunService(runs, workflows), workflows, executor


async def _drive(runs, executor, run_id, *, iters=40):
    for _ in range(iters):
        due = await runs.claim_due(1, lease_seconds=300)  # stand in for the RunWorker loop
        if not due:
            current = await runs.get(run_id)
            if current is not None and current.status in (
                RunStatus.COMPLETED,
                RunStatus.FAILED,
                RunStatus.REJECTED,
            ):
                return current
            continue
        await executor.handle(due[0])
    return await runs.get(run_id)


async def test_resource_run_reaches_completed(
    pg_session_factory: async_sessionmaker,
    servicenow: ServiceNowMock,
    airflow: AirflowMock,
    project_manager: ProjectManagerMock,
) -> None:
    runs, run_service, workflows, executor = await _assemble(
        pg_session_factory, servicenow, airflow, project_manager
    )
    await workflows.register(make_workflow(identifier="provision-vm", run_type=RunType.RESOURCE))
    run = await run_service.trigger(
        workflow_identifier="provision-vm",
        created_by="jdoe",
        max_retries=3,
        ticket_params={},
        workflow_params={},
        resource=make_resource_spec(vendor_id="vm-1"),
        operation=ResourceOperation.CREATE,
        ticket=None,
    )

    final = await _drive(runs, executor, run.run_id)

    assert final is not None and final.status is RunStatus.COMPLETED
    assert any("servicecatalog" in p for _, p in servicenow.requests)  # ticket ordered
    finalized = project_manager.patches[-1]  # resource finalized
    assert finalized["state"] == "READY" and finalized["in_progress"] is False
    assert "last_run_id" not in finalized  # the record does not carry run ids
    # The record's own id was captured from the provider's create response, so the run behind that
    # state is found by asking the orchestrator — what GET /workflow-runs/latest serves.
    record = next(iter(project_manager.resources.values()))
    latest = await runs.find_last_by_resource_id(record["_id"])
    assert latest is not None and latest.run_id == run.run_id
    assert servicenow.ritms[-1].state == 3  # RITM closed
    assert not servicenow.incidents  # no failure → no INC


async def test_resource_run_rejected_stops_without_provisioning(
    pg_session_factory: async_sessionmaker,
    servicenow: ServiceNowMock,
    airflow: AirflowMock,
    project_manager: ProjectManagerMock,
) -> None:
    servicenow.default_approval = "rejected"  # the RITM is denied on approval
    runs, run_service, workflows, executor = await _assemble(
        pg_session_factory, servicenow, airflow, project_manager
    )
    await workflows.register(make_workflow(identifier="provision-vm", run_type=RunType.RESOURCE))
    run = await run_service.trigger(
        workflow_identifier="provision-vm",
        created_by="jdoe",
        max_retries=3,
        ticket_params={},
        workflow_params={},
        resource=make_resource_spec(vendor_id="vm-1"),
        operation=ResourceOperation.CREATE,
        ticket=None,
    )

    final = await _drive(runs, executor, run.run_id)

    assert final is not None and final.status is RunStatus.REJECTED
    assert final.scheduled_at is None
    # The record created for the request is removed; nothing was provisioned.
    assert project_manager.patches == []
    assert project_manager.deletes == [f"proj-1/vm/{run.run_id}"]
    assert project_manager.resources == {}
    assert not servicenow.incidents  # a rejection is not a failure → no INC


async def test_failed_resource_run_marks_the_resource_failed(
    pg_session_factory: async_sessionmaker,
    servicenow: ServiceNowMock,
    airflow: AirflowMock,
    project_manager: ProjectManagerMock,
) -> None:
    airflow.default_state = "running"  # hold it until the failure is recorded below
    runs, run_service, workflows, executor = await _assemble(
        pg_session_factory, servicenow, airflow, project_manager
    )
    await workflows.register(make_workflow(identifier="provision-vm", run_type=RunType.RESOURCE))
    run = await run_service.trigger(
        workflow_identifier="provision-vm",
        created_by="jdoe",
        max_retries=0,
        ticket_params={},
        workflow_params={},
        resource=make_resource_spec(),
        operation=ResourceOperation.CREATE,
        ticket=None,
    )

    for _ in range(10):  # through the ticket, configure and approval to the engine
        await _drive(runs, executor, run.run_id, iters=1)
        current = await runs.get(run.run_id)
        if current is not None and current.run_state.engine_run_id is not None:
            break
    dag_run_id = (await runs.get(run.run_id)).run_state.engine_run_id
    airflow.fail(dag_run_id, task="provision_vm", responsible_group="netops", message="quota")

    final = await _drive(runs, executor, run.run_id)

    assert final is not None and final.status is RunStatus.FAILED
    # Nothing is rolled back: the record stays, marked FAILED and no longer in progress.
    record = project_manager.resources[f"proj-1/vm/{run.run_id}"]
    assert record["state"] == "FAILED" and record["in_progress"] is False
    latest = await runs.find_last_by_resource_id(record["_id"])
    assert latest is not None and latest.run_id == run.run_id


async def test_delete_resource_run_removes_the_record(
    pg_session_factory: async_sessionmaker,
    servicenow: ServiceNowMock,
    airflow: AirflowMock,
    project_manager: ProjectManagerMock,
) -> None:
    project_manager.resources["proj-1/vm/vm-1"] = {
        "vendor_id": "vm-1",
        "state": "READY",
        "in_progress": False,
        "_id": "pm-1",
    }
    runs, run_service, workflows, executor = await _assemble(
        pg_session_factory, servicenow, airflow, project_manager
    )
    await workflows.register(make_workflow(identifier="delete-vm", run_type=RunType.RESOURCE))
    run = await run_service.trigger(
        workflow_identifier="delete-vm",
        created_by="jdoe",
        max_retries=3,
        ticket_params={},
        workflow_params={},
        resource=make_resource_spec(vendor_id="vm-1", resource_id="pm-1"),
        operation=ResourceOperation.DELETE,
        ticket=None,
    )

    final = await _drive(runs, executor, run.run_id)

    assert final is not None and final.status is RunStatus.COMPLETED
    assert [p["state"] for p in project_manager.patches] == ["DELETING", "DELETED"]
    assert project_manager.deletes == ["proj-1/vm/vm-1"]
    assert project_manager.resources == {}


async def test_engine_failure_run_fails_and_escalates(
    pg_session_factory: async_sessionmaker,
    servicenow: ServiceNowMock,
    airflow: AirflowMock,
    project_manager: ProjectManagerMock,
) -> None:
    # Stay running until we explicitly fail it below — otherwise the first poll would detect
    # failure before airflow.fail() attaches the routing detail, escalating to the default team.
    airflow.default_state = "running"
    runs, run_service, workflows, executor = await _assemble(
        pg_session_factory, servicenow, airflow, project_manager
    )
    await workflows.register(
        make_workflow(
            identifier="run-automation", run_type=RunType.AUTOMATION, name="Run Automation"
        )
    )
    # The automation run attaches to a pre-existing RITM (as ServiceNow would supply on trigger).
    seeded = servicenow.seed_ritm(correlation_id="user-created")
    run = await run_service.trigger(
        workflow_identifier="run-automation",
        created_by="jdoe",
        max_retries=0,
        ticket_params={},
        workflow_params={},
        resource=None,
        operation=ResourceOperation.CREATE,
        ticket=TicketRef(ticket_id=seeded.number, native_id=seeded.sys_id),
    )

    # After the engine step is triggered, mark the specific dag run failed with routing detail.
    await _drive(runs, executor, run.run_id, iters=2)  # trigger the engine run
    dag_run_id = (await runs.get(run.run_id)).run_state.engine_run_id
    airflow.fail(dag_run_id, task="provision_vm", responsible_group="netops", message="quota")

    final = await _drive(runs, executor, run.run_id)

    assert final is not None and final.status is RunStatus.FAILED
    inc = servicenow.incidents[-1].body
    # Routed to the responsible group as a sys_id — reference fields never carry names.
    assert inc["assignment_group"] == "grpsys-netops"
    assert inc["caller_id"] == servicenow.users["jdoe"]  # and the caller as a sys_user sys_id
    assert inc["u_cloudio_failed_task"] == "provision_vm"
    assert inc["u_cloudio_flow_type"] == "Airflow:dag-x"  # engine, then the automation it ran
    # The DAG's own error reaches the responder as the incident body, titled by the workflow.
    assert inc["short_description"] == f"Error in Run Automation automation ({seeded.number})"
    assert inc["description"] == (
        f"Run {run.run_id} has failed with the following error:\nTaskException: quota"
    )
    assert any(r.work_notes for r in servicenow.ritms)  # RITM closed with a note about the incident


async def test_rolled_back_engine_run_fails_the_run_instead_of_completing_it(
    pg_session_factory: async_sessionmaker,
    servicenow: ServiceNowMock,
    airflow: AirflowMock,
    project_manager: ProjectManagerMock,
) -> None:
    # A DAG whose rollback branch cleaned up reports state `success`. Before the rollback contract
    # was read, this run completed: resource finalized, RITM closed as fulfilled, no incident —
    # for work that had been undone.
    airflow.default_state = "running"  # hold it until the rollback is recorded below
    runs, run_service, workflows, executor = await _assemble(
        pg_session_factory, servicenow, airflow, project_manager
    )
    await workflows.register(
        make_workflow(identifier="run-automation", run_type=RunType.AUTOMATION)
    )
    seeded = servicenow.seed_ritm(correlation_id="user-created")
    run = await run_service.trigger(
        workflow_identifier="run-automation",
        created_by="jdoe",
        max_retries=0,
        ticket_params={},
        workflow_params={},
        resource=None,
        operation=ResourceOperation.CREATE,
        ticket=TicketRef(ticket_id=seeded.number, native_id=seeded.sys_id),
    )

    await _drive(runs, executor, run.run_id, iters=2)  # trigger the engine run
    dag_run_id = (await runs.get(run.run_id)).run_state.engine_run_id
    airflow.rolled_back(dag_run_id, failed_tasks={"provision_vm": "netops"}, message="quota")

    final = await _drive(runs, executor, run.run_id)

    assert final is not None and final.status is RunStatus.FAILED
    inc = servicenow.incidents[-1].body
    assert inc["assignment_group"] == "grpsys-netops"  # routed by the controller's map
    assert inc["u_cloudio_failed_task"] == "provision_vm"
    assert servicenow.ritms[-1].state == 4  # RITM closed unsuccessful, not fulfilled
