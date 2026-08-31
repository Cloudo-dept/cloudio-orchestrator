"""The run driver.

Given a ``run_id``, load the run, advance it through as many synchronous steps as possible this
wake-up, and either complete it, schedule it for a later poll/retry (by setting ``scheduled_at`` —
a worker re-drives it), or mark it ``FAILED`` and escalate. It only reads and writes the run store.
"""

import logging
import random
import uuid
from datetime import timedelta

from orchestrator.config import Settings
from orchestrator.domain import (
    FailureKind,
    RunRejected,
    RunStatus,
    StaleRunError,
    StepDeadlineExceeded,
    StepFailure,
    StepName,
    WorkflowRun,
    utcnow,
)
from orchestrator.log import run_log_context
from orchestrator.orchestration.escalator import FailureEscalator
from orchestrator.orchestration.failure_policy import policy_for
from orchestrator.orchestration.plans import RUN_PLANS
from orchestrator.orchestration.steps import StepHandler
from orchestrator.ports import WorkflowRunRepository

logger = logging.getLogger(__name__)


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
            logger.warning("Run %s not found; dropping message.", run_id)
            return
        if run.status in (RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.REJECTED):
            logger.debug(
                "Run %s already terminal (%s); ignoring stale delivery.", run_id, run.status
            )
            return  # terminal: ignore a duplicate/stale delivery

        # Bind before anything else runs. Every record emitted from here down — this executor, the
        # step handlers, the adapters they call, the escalator — carries cloudio.run.*, without a
        # single signature change and without run vocabulary crossing a port.
        with run_log_context(run):
            try:
                await self._drive(run)
            except Exception as error:
                # Driving a run is total: a failure anywhere in here ends the run, visibly. Without
                # this, anything raised *around* the steps (an unknown step name, a plan naming a
                # handler that does not exist, a save that fails) escaped to the worker, which
                # logged "crashed mid-drive" and let the lease re-drive it — forever, with nothing
                # in the ticket system to show for it.
                await self._crash(run, error)

    async def _drive(self, run: WorkflowRun) -> None:
        plan = RUN_PLANS[run.run_type]
        step = StepName(run.current_step) if run.current_step else plan[0]
        run.status = RunStatus.RUNNING
        run.current_step = step  # so the very first record already carries cloudio.run.step
        logger.info(
            "Driving run %s (type=%s) starting at step %s; plan has %s steps.",
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
                "Run %s executing step %s (attempt %s, elapsed %.0fs).",
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
            except StepDeadlineExceeded as deadline:
                # Distinct from a step that merely raised: the step never got to finish inside its
                # budget. Folded into the generic handler this was indistinguishable from a
                # provider error, which sends you looking at the wrong system.
                logger.warning(
                    "Run %s step %s exceeded its %.0fs wall-clock budget after %s attempt(s).",
                    run.run_id,
                    step,
                    handler.max_step_duration_seconds,
                    attempt + 1,
                    exc_info=deadline,
                )
                await self._on_step_error(run, step, handler, deadline)
                return
            except Exception as error:
                # exc_info matters here: this is the *only* place an adapter exception is ever
                # logged, and str(IndexError) is "list index out of range" — no type, no stack, no
                # indication of which provider call produced it.
                logger.warning("Run %s step %s raised: %s", run.run_id, step, error, exc_info=error)
                await self._on_step_error(run, step, handler, error)
                return

            if not done:  # async wait → schedule a later poll
                logger.info(
                    "Run %s step %s not done yet; will poll again in %ss.",
                    run.run_id,
                    step,
                    handler.poll_interval_seconds,
                )
                await self._schedule(run, delay=handler.poll_interval_seconds)
                return

            logger.info("Run %s step %s completed.", run.run_id, step)
            run.run_state.step_attempts.pop(step, None)
            run.run_state.step_started_at.pop(step, None)
            idx = plan.index(step)
            if idx + 1 < len(plan):
                step = plan[idx + 1]
                logger.debug("Run %s advancing to next step %s.", run.run_id, step)
                continue  # run the next synchronous step now
            run.status, run.current_step, run.scheduled_at = RunStatus.COMPLETED, None, None
            await self._save(run)
            logger.info("Run %s completed all %s steps.", run.run_id, len(plan))
            return

    async def _on_step_error(
        self, run: WorkflowRun, step: StepName, handler: StepHandler, error: Exception
    ) -> None:
        attempts = run.run_state.step_attempts
        attempts[step] = attempts.get(step, 0) + 1
        # A classified failure can be one there is no point retrying (a validation error fails
        # identically on every re-run, and each RUN_ENGINE retry costs a whole fresh engine run).
        # Anything unclassified is a TASK failure: retried, exactly as before.
        kind = error.kind if isinstance(error, StepFailure) else FailureKind.TASK
        retryable = policy_for(kind).retryable
        if retryable and attempts[step] <= run.max_retries:
            handler.reset_for_retry(run.run_state)
            run.run_state.step_started_at.pop(step, None)
            backoff = self.settings.retry_base_seconds * 2 ** (attempts[step] - 1) + random.uniform(
                1, 5
            )
            await self._schedule(run, delay=backoff)
            logger.warning(
                "Run %s step %s failed (attempt %s/%s); retry in %.0fs: %s",
                run.run_id,
                step,
                attempts[step],
                run.max_retries,
                backoff,
                error,
                exc_info=error,
            )
            return
        # Exhausted (or never retryable) → terminal FAILED + escalate per the failure policy.
        # Nothing is rolled back.
        run.run_state.errors[step] = str(error)
        run.status, run.scheduled_at = RunStatus.FAILED, None
        logger.error(
            "Run %s step %s %s; escalating a %s failure and marking FAILED.",
            run.run_id,
            step,
            f"exhausted {run.max_retries} retries" if retryable else "is not retryable",
            kind.value,
            exc_info=error,
        )
        await self.escalator.escalate(run, error)
        await self._save(run)
        logger.critical(
            "Run %s step %s permanently FAILED: %s", run.run_id, step, error, exc_info=error
        )

    async def _crash(self, run: WorkflowRun, error: Exception) -> None:
        """A failure in the driving code itself, not in a step. Terminal on the first occurrence:
        an unknown step, a missing handler or a broken save recurs on every re-drive, so retrying
        only hides it for longer."""
        if run.status is RunStatus.FAILED:
            # _on_step_error already escalated this drive and then its own save threw. Escalating
            # again would open a second Incident for one failure; retry the save instead.
            logger.error(
                "Run %s failed to persist after escalation: %s", run.run_id, error, exc_info=error
            )
            await self._save_quietly(run)
            return
        if run.current_step is not None:
            run.run_state.errors[StepName(run.current_step)] = str(error)
        run.status, run.scheduled_at = RunStatus.FAILED, None
        logger.error(
            "Run %s crashed while being driven at step %s; escalating and marking FAILED.",
            run.run_id,
            run.current_step,
            exc_info=error,
        )
        await self.escalator.escalate(run, error)  # never raises
        await self._save_quietly(run)
        logger.critical("Run %s permanently FAILED: %s", run.run_id, error, exc_info=error)

    async def _save_quietly(self, run: WorkflowRun) -> None:
        """Save on the crash path. The save is often *what* crashed, so a second failure here must
        not escape — the Incident is already open, which is the part a human acts on."""
        try:
            await self._save(run)
        except Exception as save_error:
            logger.error(
                "Run %s could not be persisted as FAILED: %s",
                run.run_id,
                save_error,
                exc_info=save_error,
            )

    async def _reject(self, run: WorkflowRun, step: StepName, rejection: RunRejected) -> None:
        # The request was denied in the ticket system — a clean terminal stop, not a failure:
        # no retry, no rollback, no incident (the RITM already carries the rejection).
        run.run_state.errors[step] = str(rejection)
        run.status, run.scheduled_at = RunStatus.REJECTED, None
        await self._save(run)
        logger.info("Run %s REJECTED at step %s: %s", run.run_id, step, rejection)

    async def _schedule(self, run: WorkflowRun, delay: float) -> None:
        run.scheduled_at = utcnow() + timedelta(seconds=delay)  # a worker re-drives when due
        logger.debug("Run %s rescheduled for %s (in %.0fs).", run.run_id, run.scheduled_at, delay)
        await self._save(run)

    async def _save(self, run: WorkflowRun) -> None:
        try:
            await self.runs.save(run)
            logger.debug(
                "Run %s saved (status=%s, step=%s).", run.run_id, run.status, run.current_step
            )
        except StaleRunError as stale:
            # Lost to a concurrent writer (an overlapping re-drive) — its save already carries
            # equivalent-or-newer state, and a worker re-drives if anything is left to do. Log the
            # discarded state: without it you cannot tell what was lost, and a terminal status set
            # just above may never have been persisted.
            logger.info(
                "Run %s save lost to a concurrent writer (discarded status=%s step=%s v%s): %s",
                run.run_id,
                run.status.value,
                run.current_step,
                run.version,
                stale,
            )
