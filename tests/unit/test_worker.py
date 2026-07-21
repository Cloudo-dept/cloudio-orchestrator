"""RunWorker / OrchestratorWorker loop mechanics over the fake repository."""

import asyncio
import uuid
from collections.abc import Awaitable, Callable

from orchestrator.config import Settings
from orchestrator.domain import RunStatus, RunType, StepName, WorkflowEngineType
from orchestrator.orchestration.escalator import FailureEscalator
from orchestrator.orchestration.executor import RunExecutor
from orchestrator.orchestration.plans import build_handlers
from orchestrator.worker import OrchestratorWorker, RunWorker
from tests.factories import make_run
from tests.fakes import (
    FakeResourceManagerClient,
    FakeTicketSystemClient,
    FakeWorkflowEngineClient,
    FakeWorkflowRunRepository,
)


class RecordingExecutor:
    """Stand-in for RunExecutor: records the run_ids it is handed and marks each terminal so
    claim_due does not return it again."""

    def __init__(self, runs: FakeWorkflowRunRepository) -> None:
        self.runs = runs
        self.handled: list[uuid.UUID] = []

    async def handle(self, run_id: uuid.UUID) -> None:
        self.handled.append(run_id)
        run = await self.runs.get(run_id)
        assert run is not None
        run.status = RunStatus.COMPLETED
        run.scheduled_at = None
        await self.runs.save(run)


async def _drain(
    worker: RunWorker | OrchestratorWorker,
    done: Callable[[], Awaitable[bool]],
    *,
    timeout: float = 2.0,
) -> None:
    task = asyncio.create_task(worker.run() if isinstance(worker, RunWorker) else worker.start())
    try:

        async def wait_done() -> None:
            while not await done():
                await asyncio.sleep(0.005)

        await asyncio.wait_for(wait_done(), timeout)
    finally:
        worker.request_stop()
        await asyncio.wait_for(task, timeout)


async def test_worker_claims_one_at_a_time(runs: FakeWorkflowRunRepository) -> None:
    ids = [(await runs.create(make_run(run_type=RunType.AUTOMATION))).run_id for _ in range(3)]
    executor = RecordingExecutor(runs)
    worker = RunWorker(runs, executor, poll_interval_seconds=0.01, lease_seconds=300)  # type: ignore[arg-type]

    async def done() -> bool:
        return len(executor.handled) == 3

    await _drain(worker, done)
    assert sorted(executor.handled, key=str) == sorted(ids, key=str)  # each handled exactly once
    for run_id in ids:
        stored = await runs.get(run_id)
        assert stored is not None and stored.status is RunStatus.COMPLETED


async def test_pool_drains_without_double_claim(runs: FakeWorkflowRunRepository) -> None:
    ids = [(await runs.create(make_run(run_type=RunType.AUTOMATION))).run_id for _ in range(10)]
    executor = RecordingExecutor(runs)
    worker = OrchestratorWorker(
        runs,
        executor,
        concurrency_limit=4,  # type: ignore[arg-type]
        poll_interval_seconds=0.01,
        lease_seconds=300,
    )

    async def done() -> bool:
        return len(executor.handled) == 10

    await _drain(worker, done)
    assert len(set(executor.handled)) == 10  # no run handled twice
    assert set(executor.handled) == set(ids)


async def test_worker_stops_when_idle(runs: FakeWorkflowRunRepository) -> None:
    executor = RecordingExecutor(runs)  # nothing due at all
    worker = RunWorker(runs, executor, poll_interval_seconds=0.01, lease_seconds=300)  # type: ignore[arg-type]
    task = asyncio.create_task(worker.run())
    await asyncio.sleep(0.02)
    worker.request_stop()
    await asyncio.wait_for(task, 1.0)  # exits promptly on stop
    assert executor.handled == []


async def test_worker_drives_a_real_run_to_completion(
    runs: FakeWorkflowRunRepository,
    tickets: FakeTicketSystemClient,
    resources: FakeResourceManagerClient,
    engine: FakeWorkflowEngineClient,
    settings: Settings,
) -> None:
    handlers = build_handlers(tickets, resources, {WorkflowEngineType.AIRFLOW: engine})
    handlers[StepName.RUN_ENGINE].poll_interval_seconds = 0  # re-drive immediately
    escalator = FailureEscalator(tickets, "cloudio")
    executor = RunExecutor(handlers, runs, settings, escalator)
    run = await runs.create(make_run(run_type=RunType.AUTOMATION))
    worker = RunWorker(runs, executor, poll_interval_seconds=0.01, lease_seconds=300)

    async def done() -> bool:
        current = await runs.get(run.run_id)
        return current is not None and current.status is RunStatus.COMPLETED

    await _drain(worker, done)
    final = await runs.get(run.run_id)
    assert final is not None and final.status is RunStatus.COMPLETED
    # Automation attaches to the caller's RITM (never opens one) and closes it at the end.
    assert tickets.open_ticket_calls == [] and tickets.closed
