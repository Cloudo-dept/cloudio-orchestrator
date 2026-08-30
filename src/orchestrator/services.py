"""Use-cases the API delegates to."""

import logging
import uuid
from typing import Any

from orchestrator.domain import (
    ResolvedWorkflow,
    ResourceParamsRequired,
    ResourceSpec,
    RunState,
    RunStatus,
    RunType,
    TicketRef,
    TicketRefRequired,
    UnknownWorkflowError,
    Workflow,
    WorkflowRun,
)
from orchestrator.ports import WorkflowRepository, WorkflowRunRepository

logger = logging.getLogger(__name__)


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
        approval_group: str | None = None,
    ) -> WorkflowRun:
        logger.info("Trigger requested for workflow '%s' by %s.", workflow_identifier, created_by)
        wf = await self.workflows.get_by_identifier(workflow_identifier)
        if wf is None:
            logger.warning("Trigger rejected: unknown workflow '%s'.", workflow_identifier)
            raise UnknownWorkflowError(workflow_identifier)
        if wf.run_type is RunType.RESOURCE and resource is None:
            logger.warning(
                "Trigger rejected: workflow '%s' is a resource run but no resource was supplied.",
                workflow_identifier,
            )
            raise ResourceParamsRequired(workflow_identifier)
        if wf.run_type is RunType.AUTOMATION and ticket is None:
            logger.warning(
                "Trigger rejected: workflow '%s' is an automation run but no ticket was supplied.",
                workflow_identifier,
            )
            raise TicketRefRequired(workflow_identifier)

        state = RunState(
            workflow=ResolvedWorkflow(
                identifier=wf.identifier,
                name=wf.name,
                engine_type=wf.engine_type,
                automation_id=wf.automation_id,
                ticket_template_id=wf.ticket_template_id,
            ),
            ticket_params=ticket_params,
            workflow_params=workflow_params,
            # Only a run that opens its own ticket can assign one an approval group.
            approval_group=approval_group if wf.run_type is RunType.RESOURCE else None,
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
            "Created run %s (type=%s) for workflow '%s'; queued for immediate pickup.",
            created.run_id,
            created.run_type,
            wf.identifier,
        )
        if approval_group and created.run_state.approval_group is None:
            # Accepted, but it does nothing: this run attaches to a ticket somebody else already
            # opened, so there is none for us to route for approval. Say so rather than dropping it
            # silently, which reads as "the group I asked for was ignored for no reason".
            logger.warning(
                "Run %s is an automation run; approval_group '%s' has no effect (it opens no "
                "ticket).",
                created.run_id,
                approval_group,
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
        logger.info("Ticket callback for %s: woke %s run(s) to re-poll now.", ticket_id, woken)
        return woken

    async def wake_by_engine_run(self, engine_run_id: str) -> int:
        runs = await self.runs.find_by_engine_run_id(engine_run_id)
        woken = await self._wake_all(runs)
        logger.info("Engine callback for %s: woke %s run(s) to re-poll now.", engine_run_id, woken)
        return woken

    async def _wake_all(self, runs: list[WorkflowRun]) -> int:
        woken = 0
        for run in runs:
            if await self.runs.wake(run.run_id):  # no-op for a terminal run
                woken += 1
        return woken
