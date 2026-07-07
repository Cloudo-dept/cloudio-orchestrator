"""HTTP surface via ASGITransport, with services wired to the in-memory fakes."""

import uuid
from collections.abc import AsyncIterator
from types import SimpleNamespace

import httpx
import pytest

from orchestrator.api import app
from orchestrator.services import RunCallbackService, WorkflowRunService, WorkflowService
from tests.fakes import FakeHealthCheck, FakeWorkflowRepository, FakeWorkflowRunRepository


@pytest.fixture
async def client(
    runs: FakeWorkflowRunRepository, workflows: FakeWorkflowRepository
) -> AsyncIterator[httpx.AsyncClient]:
    app.state.container = SimpleNamespace(
        workflow_service=WorkflowService(workflows),
        run_service=WorkflowRunService(runs, workflows),
        callback_service=RunCallbackService(runs),
        health_check=FakeHealthCheck(healthy=True),
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


async def test_healthz_is_liveness_only(client: httpx.AsyncClient) -> None:
    resp = await client.get("/healthz")
    assert resp.status_code == 200 and resp.json() == {"status": "ok"}


async def test_readyz_ok_when_dependency_reachable(client: httpx.AsyncClient) -> None:
    resp = await client.get("/readyz")
    assert resp.status_code == 200 and resp.json() == {"status": "ready"}


async def test_readyz_503_when_dependency_down(client: httpx.AsyncClient) -> None:
    app.state.container.health_check = FakeHealthCheck(healthy=False)
    resp = await client.get("/readyz")
    assert resp.status_code == 503


WORKFLOW_BODY = {
    "identifier": "provision-vm",
    "run_type": "resource",
    "engine_type": "airflow",
    "automation_id": "dag-x",
    "ticket_template_id": "cat-1",
}
RESOURCE = {  # a CREATE — the caller does not supply vendor_id (assigned at ConfigureResourceStep)
    "project_id": "proj-1",
    "resource_type": "vm",
    "name": "app-01",
    "region": "gvt",
    "environment": "prod",
}


async def test_register_and_get_workflow(client: httpx.AsyncClient) -> None:
    resp = await client.post("/api/v1/workflows", json=WORKFLOW_BODY)
    assert resp.status_code == 201
    body = resp.json()
    assert body["workflow_id"] and body["run_type"] == "resource"

    got = await client.get("/api/v1/workflows/provision-vm")
    assert got.status_code == 200 and got.json()["automation_id"] == "dag-x"

    missing = await client.get("/api/v1/workflows/ghost")
    assert missing.status_code == 404


async def test_register_duplicate_identifier_conflicts(client: httpx.AsyncClient) -> None:
    assert (await client.post("/api/v1/workflows", json=WORKFLOW_BODY)).status_code == 201
    dup = await client.post("/api/v1/workflows", json=WORKFLOW_BODY)
    assert dup.status_code == 409
    assert "already registered" in dup.json()["detail"]


async def test_list_workflows(client: httpx.AsyncClient) -> None:
    assert (await client.get("/api/v1/workflows")).json() == []
    await client.post("/api/v1/workflows", json=WORKFLOW_BODY)

    listed = await client.get("/api/v1/workflows")
    assert listed.status_code == 200
    assert [wf["identifier"] for wf in listed.json()] == ["provision-vm"]


async def test_update_workflow(client: httpx.AsyncClient) -> None:
    await client.post("/api/v1/workflows", json=WORKFLOW_BODY)

    edit = {
        "run_type": "automation",
        "engine_type": "airflow",
        "automation_id": "dag-y",
        "ticket_template_id": "cat-2",
        "name": "Renamed",
    }
    resp = await client.put("/api/v1/workflows/provision-vm", json=edit)
    assert resp.status_code == 200
    body = resp.json()
    assert body["name"] == "Renamed" and body["automation_id"] == "dag-y"
    assert body["run_type"] == "automation"

    # persisted
    got = await client.get("/api/v1/workflows/provision-vm")
    assert got.json()["ticket_template_id"] == "cat-2"

    missing = await client.put("/api/v1/workflows/ghost", json=edit)
    assert missing.status_code == 404


async def test_trigger_resource_run(client: httpx.AsyncClient) -> None:
    await client.post("/api/v1/workflows", json=WORKFLOW_BODY)
    resp = await client.post(
        "/api/v1/workflow-runs",
        json={"workflow_identifier": "provision-vm", "created_by": "jdoe", "resource": RESOURCE},
    )
    assert resp.status_code == 201
    body = resp.json()
    assert body["run_id"] and body["run_type"] == "resource" and body["status"] == "pending"
    # The typed run_state is exposed with a real schema. vendor_id is still empty at trigger time —
    # ConfigureResourceStep assigns the run id when the run is driven.
    assert body["run_state"]["resource"]["vendor_id"] == ""

    run_id = body["run_id"]
    got = await client.get(f"/api/v1/workflow-runs/{run_id}")
    assert got.status_code == 200 and got.json()["run_id"] == run_id


async def test_trigger_unknown_workflow_is_404(client: httpx.AsyncClient) -> None:
    resp = await client.post(
        "/api/v1/workflow-runs", json={"workflow_identifier": "ghost", "created_by": "jdoe"}
    )
    assert resp.status_code == 404


async def test_trigger_resource_workflow_without_resource_is_422(client: httpx.AsyncClient) -> None:
    await client.post("/api/v1/workflows", json=WORKFLOW_BODY)
    resp = await client.post(
        "/api/v1/workflow-runs", json={"workflow_identifier": "provision-vm", "created_by": "jdoe"}
    )
    assert resp.status_code == 422
    assert "resource is required" in resp.json()["detail"]


async def test_malformed_resource_spec_is_422_at_boundary(client: httpx.AsyncClient) -> None:
    await client.post("/api/v1/workflows", json=WORKFLOW_BODY)
    bad = {k: v for k, v in RESOURCE.items() if k != "name"}  # drop a required field
    resp = await client.post(
        "/api/v1/workflow-runs",
        json={"workflow_identifier": "provision-vm", "created_by": "jdoe", "resource": bad},
    )
    assert resp.status_code == 422


async def test_get_unknown_run_is_404(client: httpx.AsyncClient) -> None:
    resp = await client.get(f"/api/v1/workflow-runs/{'0' * 8}-0000-0000-0000-000000000000")
    assert resp.status_code == 404


async def _make_waiting_run(
    runs: FakeWorkflowRunRepository,
    *,
    ticket_id: str | None = None,
    engine_run_id: str | None = None,
) -> uuid.UUID:
    """A RUNNING run parked on a far-future poll — the state a callback wakes early."""
    from datetime import timedelta

    from orchestrator.domain import RunStatus, RunType, TicketRef, utcnow
    from tests.factories import make_run

    run = make_run(run_type=RunType.RESOURCE)
    run.status = RunStatus.RUNNING
    run.scheduled_at = utcnow() + timedelta(days=1)
    if ticket_id is not None:
        run.run_state.ticket = TicketRef(ticket_id=ticket_id, native_id="sys-1")
    run.run_state.engine_run_id = engine_run_id
    await runs.create(run)
    return run.run_id


async def test_ticket_approval_callback_wakes_the_run(
    client: httpx.AsyncClient, runs: FakeWorkflowRunRepository
) -> None:
    from orchestrator.domain import utcnow

    run_id = await _make_waiting_run(runs, ticket_id="RITM0001234")
    resp = await client.post(
        "/api/v1/callbacks/ticket-approval", json={"ticket_id": "RITM0001234"}
    )
    assert resp.status_code == 202 and resp.json() == {"woken": 1}
    woken = await runs.get(run_id)
    assert woken is not None and woken.scheduled_at is not None
    assert woken.scheduled_at <= utcnow()  # now due → a worker re-polls immediately


async def test_engine_run_callback_wakes_the_run(
    client: httpx.AsyncClient, runs: FakeWorkflowRunRepository
) -> None:
    from orchestrator.domain import utcnow

    run_id = await _make_waiting_run(runs, engine_run_id="dagrun-7")
    resp = await client.post("/api/v1/callbacks/engine-run", json={"engine_run_id": "dagrun-7"})
    assert resp.status_code == 202 and resp.json() == {"woken": 1}
    woken = await runs.get(run_id)
    assert woken is not None and woken.scheduled_at is not None
    assert woken.scheduled_at <= utcnow()


async def test_callback_for_unknown_reference_is_a_noop(client: httpx.AsyncClient) -> None:
    ticket = await client.post(
        "/api/v1/callbacks/ticket-approval", json={"ticket_id": "RITM-ghost"}
    )
    assert ticket.status_code == 202 and ticket.json() == {"woken": 0}
    engine = await client.post("/api/v1/callbacks/engine-run", json={"engine_run_id": "ghost"})
    assert engine.status_code == 202 and engine.json() == {"woken": 0}


async def test_list_by_resource_and_ticket(client: httpx.AsyncClient) -> None:
    await client.post("/api/v1/workflows", json=WORKFLOW_BODY)
    # An UPDATE targets an existing record, so the caller supplies vendor_id (a CREATE would not).
    resource = {**RESOURCE, "operation": "update", "vendor_id": "vm-1"}
    await client.post(
        "/api/v1/workflow-runs",
        json={"workflow_identifier": "provision-vm", "created_by": "jdoe", "resource": resource},
    )

    by_resource = await client.get("/api/v1/workflow-runs", params={"resource_id": "vm-1"})
    assert by_resource.status_code == 200 and len(by_resource.json()) == 1

    empty = await client.get("/api/v1/workflow-runs", params={"ticket_id": "RITM-absent"})
    assert empty.status_code == 200 and empty.json() == []

    unfiltered = await client.get("/api/v1/workflow-runs")
    assert unfiltered.status_code == 200 and len(unfiltered.json()) == 1
