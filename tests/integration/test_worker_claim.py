"""RunWorker driving real Postgres-backed runs to completion (Docker-gated)."""

import asyncio

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker

from orchestrator.adapters.database import PostgresWorkflowRunRepository
from orchestrator.config import Settings
from orchestrator.domain import RunStatus, RunType, StepName, WorkflowEngineType
from orchestrator.orchestration.escalator import FailureEscalator
from orchestrator.orchestration.executor import RunExecutor
from orchestrator.orchestration.plans import build_handlers
from orchestrator.worker import RunWorker
from tests.factories import make_run
from tests.fakes import (
    FakeResourceManagerClient,
    FakeTicketSystemClient,
    FakeWorkflowEngineClient,
)

pytestmark = pytest.mark.integration


async def test_worker_drives_pg_run_to_completion(
    pg_session_factory: async_sessionmaker,
) -> None:
    runs = PostgresWorkflowRunRepository(pg_session_factory)
    tickets, resources, engine = (
        FakeTicketSystemClient(),
        FakeResourceManagerClient(),
        FakeWorkflowEngineClient(),
    )
    handlers = build_handlers(tickets, resources, {WorkflowEngineType.AIRFLOW: engine})
    handlers[StepName.RUN_ENGINE].poll_interval_seconds = 0
    executor = RunExecutor(
        handlers,
        runs,
        Settings.model_construct(retry_base_seconds=1.0),
        FailureEscalator(tickets, "cloudio"),
    )
    run = await runs.create(make_run(run_type=RunType.RESOURCE))
    worker = RunWorker(runs, executor, poll_interval_seconds=0.01, lease_seconds=300)

    task = asyncio.create_task(worker.run())
    try:
        for _ in range(400):
            current = await runs.get(run.run_id)
            if current is not None and current.status is RunStatus.COMPLETED:
                break
            await asyncio.sleep(0.01)
    finally:
        worker.request_stop()
        await asyncio.wait_for(task, 2.0)

    final = await runs.get(run.run_id)
    assert final is not None and final.status is RunStatus.COMPLETED
    assert final.run_state.resource_finalized is True
    assert tickets.closed and resources.create_calls
