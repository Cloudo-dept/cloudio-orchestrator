"""The daemon: a pool of identical RunWorker loops that claim and drive runs directly.

Each loop claims one due run (claim_due(1), FOR UPDATE SKIP LOCKED), drives it through
RunExecutor.handle, and claims again — no scheduler, no queue. The re-drive lease on claim_due is
what makes a crash re-drive.
"""

import asyncio
import itertools
import logging

from orchestrator.log import worker_log_context
from orchestrator.orchestration.executor import RunExecutor
from orchestrator.ports import WorkflowRunRepository

_worker_ids = itertools.count()  # stable per-loop tag for log correlation
logger = logging.getLogger(__name__)


class RunWorker:
    """One claim-and-drive loop: pull a single due run, drive it, repeat."""

    def __init__(
        self,
        runs: WorkflowRunRepository,
        executor: RunExecutor,
        *,
        poll_interval_seconds: float,
        lease_seconds: float,
    ) -> None:
        self.runs = runs
        self.executor = executor
        self.poll_interval = poll_interval_seconds
        self.lease_seconds = lease_seconds
        self.worker_id = next(_worker_ids)
        self._stop = asyncio.Event()

    def request_stop(self) -> None:
        logger.debug("Worker loop #%s stop requested.", self.worker_id)
        self._stop.set()

    async def run(self) -> None:
        with worker_log_context(self.worker_id):
            logger.info("Worker loop #%s started; polling for due runs.", self.worker_id)
            while not self._stop.is_set():
                try:
                    run_ids = await self.runs.claim_due(1, self.lease_seconds)  # 0 or 1 due run
                except Exception:
                    # The claim used to sit outside any try. A transient database error escaped
                    # this loop, propagated through the gather in OrchestratorWorker.start (no
                    # return_exceptions) and out of asyncio.run, killing the whole daemon with a
                    # raw interpreter traceback on stderr — never through logging, so no ECS
                    # record at all: the log stream simply stopped. A failed claim is a retryable
                    # condition, not a reason to take the process down.
                    logger.exception(
                        "Worker loop #%s: claim failed; retrying in %ss.",
                        self.worker_id,
                        self.poll_interval,
                    )
                    await self._idle()
                    continue
                if not run_ids:  # nothing due → wait a tick
                    logger.debug(
                        "Worker loop #%s found no due run; sleeping %ss.",
                        self.worker_id,
                        self.poll_interval,
                    )
                    await self._idle()
                    continue
                run_id = run_ids[0]
                logger.info("Worker loop #%s claimed run %s; driving it.", self.worker_id, run_id)
                try:
                    await self.executor.handle(run_id)
                    logger.debug("Worker loop #%s finished driving run %s.", self.worker_id, run_id)
                except Exception:
                    logger.exception(
                        "Worker loop #%s: run %s crashed mid-drive; the lease will re-drive it.",
                        self.worker_id,
                        run_id,
                    )
            logger.info("Worker loop #%s stopped.", self.worker_id)

    async def _idle(self) -> None:
        """Wait one poll interval, returning early if a stop was requested."""
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=self.poll_interval)
        except TimeoutError:
            pass


class OrchestratorWorker:
    """The daemon: runs `concurrency_limit` identical RunWorker loops concurrently. Each claims
    its own work via SKIP LOCKED, so the loops — and multiple daemons — never collide."""

    def __init__(
        self,
        runs: WorkflowRunRepository,
        executor: RunExecutor,
        *,
        concurrency_limit: int,
        poll_interval_seconds: float,
        lease_seconds: float,
    ) -> None:
        self._workers = [
            RunWorker(
                runs,
                executor,
                poll_interval_seconds=poll_interval_seconds,
                lease_seconds=lease_seconds,
            )
            for _ in range(concurrency_limit)
        ]

    def request_stop(self) -> None:
        logger.warning("Stopping daemon: signalling all %s worker loops.", len(self._workers))
        for worker in self._workers:
            worker.request_stop()

    async def start(self) -> None:
        logger.info("Daemon started; %s loops claiming + driving runs.", len(self._workers))
        await asyncio.gather(*(worker.run() for worker in self._workers))
        logger.info("Daemon stopped; all worker loops drained.")
