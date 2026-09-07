"""Use-cases the API delegates to."""

import uuid
from collections.abc import Mapping
from typing import Any

from loguru import logger

from orchestrator.domain import (
    ResolvedWorkflow,
    ResourceParamsRequired,
    ResourceSpec,
    RunNotFound,
    RunNotRetryable,
    RunState,
    RunStatus,
    RunType,
    StepName,
    TicketRef,
    TicketRefRequired,
    UnknownWorkflowError,
    Workflow,
    WorkflowRun,
    utcnow,
)
from orchestrator.orchestration.steps import StepHandler
from orchestrator.ports import TicketSystemClient, WorkflowRepository, WorkflowRunRepository


class WorkflowService:
    def __init__(self, workflows: WorkflowRepository) -> None:
        self.workflows = workflows

    async def register(self, workflow: Workflow) -> Workflow:
        return await self.workflows.register(workflow)

    async def get(self, identifier: str) -> Workflow | None:
        return await self.workflows.get_by_identifier(identifier)

    async def update(self, workflow: Workflow) -> Workflow | None:
        return await self.workflows.update(workflow)

    async def list(self) -> list[Workflow]:
        return await self.workflows.list()


class WorkflowRunService:
    def __init__(self, runs: WorkflowRunRepository, workflows: WorkflowRepository) -> None:
        self.runs = runs
        self.workflows = workflows

    async def trigger(
        self,
        *,
        workflow_identifier: str,
        created_by: str,
        max_retries: int,
        ticket_params: dict[str, Any],
        workflow_params: dict[str, Any],
        resource: ResourceSpec | None,
        ticket: TicketRef | None,
    ) -> WorkflowRun:
        logger.info("Trigger requested for workflow '{}' by {}.", workflow_identifier, created_by)
        wf = await self.workflows.get_by_identifier(workflow_identifier)
        if wf is None:
            logger.warning("Trigger rejected: unknown workflow '{}'.", workflow_identifier)
            raise UnknownWorkflowError(workflow_identifier)
        if wf.run_type is RunType.RESOURCE and resource is None:
            logger.warning(
                "Trigger rejected: workflow '{}' is a resource run but no resource was supplied.",
                workflow_identifier,
            )
            raise ResourceParamsRequired(workflow_identifier)
        if wf.run_type is RunType.AUTOMATION and ticket is None:
            logger.warning(
                "Trigger rejected: workflow '{}' is an automation run but no ticket was supplied.",
                workflow_identifier,
            )
            raise TicketRefRequired(workflow_identifier)

        state = RunState(
            workflow=ResolvedWorkflow(
                identifier=wf.identifier,
                engine_type=wf.engine_type,
                automation_id=wf.automation_id,
                ticket_template_id=wf.ticket_template_id,
            ),
            ticket_params=ticket_params,
            workflow_params=workflow_params,
            resource=resource if wf.run_type is RunType.RESOURCE else None,
            # Automation runs attach to the caller's existing RITM; resource runs create their own.
            ticket=ticket if wf.run_type is RunType.AUTOMATION else None,
        )
        # scheduled_at defaults to now → a RunWorker claims it on its next scan.
        run = WorkflowRun(
            run_type=wf.run_type,
            status=RunStatus.PENDING,
            workflow_identifier=wf.identifier,
            created_by=created_by,
            max_retries=max_retries,
            run_state=state,
        )
        created = await self.runs.create(run)
        logger.info(
            "Created run {} (type={}) for workflow '{}'; queued for immediate pickup.",
            created.run_id,
            created.run_type,
            wf.identifier,
        )
        return created

    async def get(self, run_id: uuid.UUID) -> WorkflowRun | None:
        return await self.runs.get(run_id)

    async def list_recent(self, limit: int = 200) -> list[WorkflowRun]:
        return await self.runs.list_recent(limit)

    async def find_by_ticket_id(self, ticket_id: str) -> list[WorkflowRun]:
        return await self.runs.find_by_ticket_id(ticket_id)

    async def find_by_resource_id(self, vendor_id: str) -> list[WorkflowRun]:
        return await self.runs.find_by_resource_id(vendor_id)


class RunRetryService:
    """Resume a FAILED run from the step it stopped at (an operator action from the console).

    A failed run keeps everything it had achieved: ``current_step`` is the step that exhausted its
    retries, and every step before it left its idempotency marker in ``RunState`` (``ticket``,
    ``resource_configured``, ``engine_run_id``, …). So resuming is not a re-trigger — it clears
    only the failed step's own bookkeeping (attempt count, wall-clock deadline, recorded error)
    plus whatever that step's handler considers stale, then makes the run due now. A worker claims
    it and drives it from ``current_step``; the completed steps ahead of it short-circuit.

    The handler map is the same one the executor drives with — a retry resets step-scoped state
    through the very hook (``reset_for_retry``) that an automatic retry uses, so the two paths
    cannot drift.

    The ticket system is told too: the run's ticket goes back to an in-progress state with a note
    saying the run was retried, so the requester sees the request is being worked again rather
    than a record that silently starts moving. The incident raised for the failure is deliberately
    left open — the escalator decides what becomes of it when (and only if) the run fails again.
    """

    def __init__(
        self,
        runs: WorkflowRunRepository,
        handlers: Mapping[StepName, StepHandler],
        tickets: TicketSystemClient,
    ) -> None:
        self.runs = runs
        self.handlers = handlers
        self.tickets = tickets

    async def retry(self, run_id: uuid.UUID) -> WorkflowRun:
        run = await self.runs.get(run_id)
        if run is None:
            logger.warning("Retry requested for unknown run {}.", run_id)
            raise RunNotFound(str(run_id))
        if run.status is not RunStatus.FAILED:
            logger.warning("Retry refused for run {}: status is {}.", run_id, run.status)
            raise RunNotRetryable(f"Run {run_id} is {run.status.value}, not failed.")

        st = run.run_state
        step = StepName(run.current_step) if run.current_step else None
        if step is not None:  # give the failed step a clean slate: attempts, deadline, error
            st.step_attempts.pop(step, None)
            st.step_started_at.pop(step, None)
            st.errors.pop(step, None)
            self.handlers[step].reset_for_retry(st)
        st.manual_retries += 1
        await self._reopen_ticket(run, step)

        # RUNNING + due now: a worker claims it on its next scan and drives it from current_step.
        run.status, run.scheduled_at = RunStatus.RUNNING, utcnow()
        await self.runs.save(run)
        logger.info(
            "Run {} retried (manual retry #{}); resuming at step {}.",
            run.run_id,
            st.manual_retries,
            step or "the start of the plan",
        )
        return run

    async def _reopen_ticket(self, run: WorkflowRun, step: StepName | None) -> None:
        """Put the retry on the run's ticket and return it to an in-progress state.

        Best-effort: a ticket system that is down must not stop an operator from resuming a run,
        so a failure here is logged and the retry carries on. Applies to both run types — an
        automation run's RITM belongs to the caller, but it is still the record tracking this
        work, so it is told the work resumed.
        """
        st = run.run_state
        if st.ticket is None:  # a resource run that failed before it ever opened its RITM
            return
        at = f"step '{step.value}'" if step else "the first step of the plan"
        note = f"Run {run.run_id} was retried (retry #{st.manual_retries}); resuming at {at}."
        try:
            await self.tickets.reopen_ticket(st.ticket, note=note)
        except Exception as error:
            logger.warning(
                "Run {}: could not re-open ticket {} for the retry ({}); resuming anyway.",
                run.run_id,
                st.ticket.ticket_id,
                error,
            )
            return
        # Re-opened, so the run owes it a close again — CLOSE_TICKET re-runs at the end.
        st.ticket_closed = False
        logger.info(
            "Run {}: ticket {} re-opened and noted for retry #{}.",
            run.run_id,
            st.ticket.ticket_id,
            st.manual_retries,
        )


class RunCallbackService:
    """Wake-early on an external notification: a callback resolves the waiting run by a neutral
    external reference and makes it due now, so a worker re-drives it immediately instead of
    waiting out the poll interval. It never trusts a status from the callback — the re-driven poll
    step still reads the authoritative state from the provider, so polling stays the source of
    truth and a lost/duplicate callback is harmless."""

    def __init__(self, runs: WorkflowRunRepository) -> None:
        self.runs = runs

    async def wake_by_ticket(self, ticket_id: str) -> int:
        runs = await self.runs.find_by_ticket_id(ticket_id)
        woken = await self._wake_all(runs)
        logger.info("Ticket callback for {}: woke {} run(s) to re-poll now.", ticket_id, woken)
        return woken

    async def wake_by_engine_run(self, engine_run_id: str) -> int:
        runs = await self.runs.find_by_engine_run_id(engine_run_id)
        woken = await self._wake_all(runs)
        logger.info("Engine callback for {}: woke {} run(s) to re-poll now.", engine_run_id, woken)
        return woken

    async def _wake_all(self, runs: list[WorkflowRun]) -> int:
        woken = 0
        for run in runs:
            if await self.runs.wake(run.run_id):  # no-op for a terminal run
                woken += 1
        return woken
