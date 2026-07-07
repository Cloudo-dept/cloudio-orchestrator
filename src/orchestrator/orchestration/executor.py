"""The run driver.

Given a ``run_id``, load the run, advance it through as many synchronous steps as possible this
wake-up, and either complete it, schedule it for a later poll/retry (by setting ``scheduled_at`` —
a worker re-drives it), or mark it ``FAILED`` and escalate. It only reads and writes the run store.
"""

import random
import uuid
from datetime import timedelta

from loguru import logger

from orchestrator.config import Settings
from orchestrator.domain import (
    RunRejected,
    RunStatus,
    StaleRunError,
    StepDeadlineExceeded,
    StepName,
    WorkflowRun,
    utcnow,
)
from orchestrator.orchestration.escalator import FailureEscalator
from orchestrator.orchestration.plans import RUN_PLANS
from orchestrator.orchestration.steps import StepHandler
from orchestrator.ports import WorkflowRunRepository


class RunExecutor:
    def __init__(
        self,
        handlers: dict[StepName, StepHandler],
        runs: WorkflowRunRepository,
        settings: Settings,
        escalator: FailureEscalator,
    ) -> None:
        self.handlers = handlers
        self.runs = runs
        self.settings = settings
        self.escalator = escalator

    async def handle(self, run_id: uuid.UUID) -> None:
        run = await self.runs.get(run_id)
        if run is None:
            logger.warning("Run {} not found; dropping message.", run_id)
            return
        if run.status in (RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.REJECTED):
            logger.debug(
                "Run {} already terminal ({}); ignoring stale delivery.", run_id, run.status
            )
            return  # terminal: ignore a duplicate/stale delivery

        plan = RUN_PLANS[run.run_type]
        step = StepName(run.current_step) if run.current_step else plan[0]
        run.status = RunStatus.RUNNING
        logger.info(
            "Driving run {} (type={}) starting at step {}; plan has {} steps.",
            run.run_id,
            run.run_type,
            step,
            len(plan),
        )

        while True:  # advance through synchronous steps
            run.current_step = step
            handler = self.handlers[step]
            started = run.run_state.step_started_at.setdefault(step, utcnow())
            attempt = run.run_state.step_attempts.get(step, 0)
            logger.info(
                "Run {} executing step {} (attempt {}, elapsed {:.0f}s).",
                run.run_id,
                step,
                attempt + 1,
                (utcnow() - started).total_seconds(),
            )
            try:
                if utcnow() - started > timedelta(seconds=handler.max_step_duration_seconds):
                    raise StepDeadlineExceeded(f"Step {step} exceeded its wall-clock budget")
                done = await handler.execute(run)
            except RunRejected as rejection:
                await self._reject(run, step, rejection)
                return
            except Exception as error:
                logger.warning("Run {} step {} raised: {}", run.run_id, step, error)
                await self._on_step_error(run, step, handler, error)
                return

            if not done:  # async wait → schedule a later poll
                logger.info(
                    "Run {} step {} not done yet; will poll again in {}s.",
                    run.run_id,
                    step,
                    handler.poll_interval_seconds,
                )
                await self._schedule(run, delay=handler.poll_interval_seconds)
                return

            logger.info("Run {} step {} completed.", run.run_id, step)
            run.run_state.step_attempts.pop(step, None)
            run.run_state.step_started_at.pop(step, None)
            idx = plan.index(step)
            if idx + 1 < len(plan):
                step = plan[idx + 1]
                logger.debug("Run {} advancing to next step {}.", run.run_id, step)
                continue  # run the next synchronous step now
            run.status, run.current_step, run.scheduled_at = RunStatus.COMPLETED, None, None
            await self._save(run)
            logger.success("Run {} completed all {} steps.", run.run_id, len(plan))
            return

    async def _on_step_error(
        self, run: WorkflowRun, step: StepName, handler: StepHandler, error: Exception
    ) -> None:
        attempts = run.run_state.step_attempts
        attempts[step] = attempts.get(step, 0) + 1
        if attempts[step] <= run.max_retries:
            handler.reset_for_retry(run.run_state)
            run.run_state.step_started_at.pop(step, None)
            backoff = self.settings.retry_base_seconds * 2 ** (attempts[step] - 1) + random.uniform(
                1, 5
            )
            await self._schedule(run, delay=backoff)
            logger.warning(
                "Run {} step {} failed (attempt {}/{}); retry in {:.0f}s: {}",
                run.run_id,
                step,
                attempts[step],
                run.max_retries,
                backoff,
                error,
            )
            return
        # exhausted → terminal FAILED + escalate (Incident + RITM note). Nothing is rolled back.
        run.run_state.errors[step] = str(error)
        run.status, run.scheduled_at = RunStatus.FAILED, None
        logger.error(
            "Run {} step {} exhausted {} retries; escalating and marking FAILED.",
            run.run_id,
            step,
            run.max_retries,
        )
        await self.escalator.escalate(run, error)
        await self._save(run)
        logger.critical("Run {} step {} permanently FAILED: {}", run.run_id, step, error)

    async def _reject(self, run: WorkflowRun, step: StepName, rejection: RunRejected) -> None:
        # The request was denied in the ticket system — a clean terminal stop, not a failure:
        # no retry, no rollback, no incident (the RITM already carries the rejection).
        run.run_state.errors[step] = str(rejection)
        run.status, run.scheduled_at = RunStatus.REJECTED, None
        await self._save(run)
        logger.info("Run {} REJECTED at step {}: {}", run.run_id, step, rejection)

    async def _schedule(self, run: WorkflowRun, delay: float) -> None:
        run.scheduled_at = utcnow() + timedelta(seconds=delay)  # a worker re-drives when due
        logger.debug("Run {} rescheduled for {} (in {:.0f}s).", run.run_id, run.scheduled_at, delay)
        await self._save(run)

    async def _save(self, run: WorkflowRun) -> None:
        try:
            await self.runs.save(run)
            logger.trace(
                "Run {} saved (status={}, step={}).", run.run_id, run.status, run.current_step
            )
        except StaleRunError:
            # Lost to a concurrent writer (an overlapping re-drive) — its save already carries
            # equivalent-or-newer state, and a worker re-drives if anything is left to do.
            logger.info(
                "Run {} save lost to a concurrent writer; a worker will re-drive.", run.run_id
            )
