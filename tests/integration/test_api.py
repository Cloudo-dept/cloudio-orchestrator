"""HTTP surface via ASGITransport, with services wired to the in-memory fakes."""

from collections.abc import AsyncIterator
from types import SimpleNamespace

import httpx
import pytest

from orchestrator.api import app
from orchestrator.services import WorkflowRunService, WorkflowService
from tests.fakes import FakeHealthCheck, FakeWorkflowRepository, FakeWorkflowRunRepository


@pytest.fixture
async def client(
    runs: FakeWorkflowRunRepository, workflows: FakeWorkflowRepository
) -> AsyncIterator[httpx.AsyncClient]:
    app.state.container = SimpleNamespace(
        workflow_service=WorkflowService(workflows),
        run_service=WorkflowRunService(runs, workflows),
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
