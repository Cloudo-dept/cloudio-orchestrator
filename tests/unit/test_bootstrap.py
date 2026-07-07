"""bootstrap.build() wires a Container from Settings (no DB connection is opened lazily)."""

import pytest

import orchestrator.cli  # noqa: F401  (smoke import: the CLI wires bootstrap + typer + uvicorn)
from orchestrator.adapters.database import PostgresHealthCheck
from orchestrator.bootstrap import Container, build
from orchestrator.config import Settings
from orchestrator.services import WorkflowRunService, WorkflowService
from orchestrator.worker import OrchestratorWorker

REQUIRED_ENV = {
    "ORCH_DATABASE_URL": "postgresql+asyncpg://u:pw@localhost/db",
    "ORCH_AIRFLOW_BASE_URL": "https://airflow.example",
    "ORCH_AIRFLOW_USERNAME": "airflow",
    "ORCH_AIRFLOW_PASSWORD": "af-secret",
    "ORCH_SERVICENOW_BASE_URL": "https://sn.example",
    "ORCH_SERVICENOW_USERNAME": "snuser",
    "ORCH_SERVICENOW_PASSWORD": "sn-secret",
    "ORCH_PM_BASE_URL": "https://pm.example",
    "ORCH_PM_TOKEN": "pm-token",
    "ORCH_WORKER_CONCURRENCY_LIMIT": "3",
}


@pytest.fixture
def env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key, value in REQUIRED_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.chdir("/tmp")


async def test_build_wires_the_container(env: None) -> None:
    container = await build(Settings())
    assert isinstance(container, Container)
    assert isinstance(container.workflow_service, WorkflowService)
    assert isinstance(container.run_service, WorkflowRunService)
    assert isinstance(container.worker, OrchestratorWorker)
    assert isinstance(container.health_check, PostgresHealthCheck)
    # concurrency_limit → that many RunWorker loops.
    assert len(container.worker._workers) == 3
