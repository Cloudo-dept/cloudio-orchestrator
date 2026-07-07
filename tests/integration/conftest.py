"""Fixtures: each provider mock + a real adapter pointed at it via ASGITransport (no sockets).

Also the Postgres fixtures for the repository/worker/e2e tests — skipped when Docker is absent.
"""

from collections.abc import AsyncIterator, Iterator

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlmodel import SQLModel

import orchestrator.domain  # noqa: F401  (populates SQLModel.metadata)
from orchestrator.adapters.airflow import AirflowWorkflowEngineClient
from orchestrator.adapters.project_manager import ProjectManagerResourceClient
from orchestrator.adapters.servicenow import ServiceNowTicketClient
from tests.mocks.airflow import AirflowMock
from tests.mocks.base import asgi
from tests.mocks.project_manager import ProjectManagerMock
from tests.mocks.servicenow import ServiceNowMock


@pytest.fixture
def airflow() -> AirflowMock:
    return AirflowMock()


@pytest.fixture
def airflow_client(airflow: AirflowMock) -> AirflowWorkflowEngineClient:
    return AirflowWorkflowEngineClient(
        base_url="http://airflow.local", username="u", password="p", transport=asgi(airflow.app)
    )


@pytest.fixture
def servicenow() -> ServiceNowMock:
    return ServiceNowMock()


@pytest.fixture
def servicenow_client(servicenow: ServiceNowMock) -> ServiceNowTicketClient:
    return ServiceNowTicketClient(
        base_url="http://servicenow.local",
        username="u",
        password="p",
        responsible_groups={"netops": "CloudIO NetOps"},
        transport=asgi(servicenow.app),
    )


@pytest.fixture
def project_manager() -> ProjectManagerMock:
    return ProjectManagerMock()


@pytest.fixture
def pm_client(project_manager: ProjectManagerMock) -> ProjectManagerResourceClient:
    return ProjectManagerResourceClient(
        base_url="http://pm.local", token="t", transport=asgi(project_manager.app)
    )


# --- Postgres (testcontainers) — skipped when Docker is unavailable ---

_ENUM_DDL = [
    "CREATE TYPE run_type AS ENUM ('automation', 'resource')",
    "CREATE TYPE run_status AS ENUM ('pending','running','completed','failed','rejected')",
    "CREATE TYPE workflow_engine_type AS ENUM ('airflow')",
]
_INDEX_DDL = [
    "CREATE INDEX idx_runs_scheduled_at ON workflow_runs (scheduled_at) "
    "WHERE scheduled_at IS NOT NULL",
    "CREATE INDEX idx_runs_ticket_id ON workflow_runs ((run_state #>> '{ticket,ticket_id}')) "
    "WHERE (run_state #>> '{ticket,ticket_id}') IS NOT NULL",
    "CREATE INDEX idx_runs_resource_vendor_id ON workflow_runs "
    "((run_state #>> '{resource,vendor_id}')) "
    "WHERE (run_state #>> '{resource,vendor_id}') IS NOT NULL",
    "CREATE INDEX idx_runs_engine_run_id ON workflow_runs "
    "((run_state #>> '{engine_run_id}')) "
    "WHERE (run_state #>> '{engine_run_id}') IS NOT NULL",
]


@pytest.fixture(scope="session")
def postgres_url() -> Iterator[str]:
    try:
        from testcontainers.postgres import PostgresContainer
    except Exception as e:  # pragma: no cover
        pytest.skip(f"testcontainers unavailable: {e}")
    try:
        container = PostgresContainer("postgres:16-alpine", driver="asyncpg")
        container.start()
    except Exception as e:  # pragma: no cover - no Docker daemon
        pytest.skip(f"Docker not available for Postgres integration tests: {e}")
    try:
        yield container.get_connection_url()
    finally:
        container.stop()


@pytest.fixture
async def pg_session_factory(postgres_url: str) -> AsyncIterator[async_sessionmaker]:
    """A fresh schema (enums + tables + indexes, mirroring 0001_initial) per test."""
    engine = create_async_engine(postgres_url)
    async with engine.begin() as conn:
        for ddl in _ENUM_DDL:
            await conn.execute(text(ddl))
        await conn.run_sync(SQLModel.metadata.create_all)
        for ddl in _INDEX_DDL:
            await conn.execute(text(ddl))
    yield async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.drop_all)
        for enum in ("workflow_engine_type", "run_status", "run_type"):
            await conn.execute(text(f"DROP TYPE IF EXISTS {enum}"))
    await engine.dispose()
