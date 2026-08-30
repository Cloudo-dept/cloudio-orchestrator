"""The ABCs every adapter implements: two repositories, three external clients, and a health probe.

Business logic depends only on these ports; every technology-specific class lives in ``adapters/``
and implements one of them. No provider vocabulary crosses a port boundary.
"""

import abc
import uuid
from typing import Any

from orchestrator.domain import (
    ApprovalStatus,
    EngineFailure,
    EngineRunStatus,
    TicketOutcome,
    TicketRef,
    Workflow,
    WorkflowRun,
)


class HealthCheck(abc.ABC):
    """A liveness/readiness probe for a backing dependency (the run store is one)."""

    @abc.abstractmethod
    async def ping(self) -> None:
        """Return normally if the dependency is reachable; raise otherwise."""


class WorkflowRunRepository(abc.ABC):
    """Durable run state. Typed on the WorkflowRun entity; no queue mechanics."""

    @abc.abstractmethod
    async def create(self, run: WorkflowRun) -> WorkflowRun: ...

    @abc.abstractmethod
    async def get(self, run_id: uuid.UUID) -> WorkflowRun | None: ...

    @abc.abstractmethod
    async def save(self, run: WorkflowRun) -> None:
        """Persist run state (incl. scheduled_at) under optimistic concurrency;
        raise StaleRunError on version conflict."""

    @abc.abstractmethod
    async def claim_due(self, limit: int, lease_seconds: float) -> list[uuid.UUID]:
        """Atomically claim runs whose scheduled_at is due: push scheduled_at forward by a
        re-drive lease (so a crash re-drives them) and return their ids for the caller (a
        worker) to drive. Workers pass limit=1 to claim a single run each."""

    @abc.abstractmethod
    async def wake(self, run_id: uuid.UUID) -> bool:
        """Make a non-terminal run due now (set scheduled_at to now) so a worker re-drives it
        immediately, and return whether a row was woken. A deliberate nudge OUTSIDE the optimistic
        version scheme: it never raises StaleRunError. Used by callbacks to shorten the wait between
        polls — the re-driven poll step is still what reads the authoritative external status."""

    @abc.abstractmethod
    async def find_by_ticket_id(self, ticket_id: str) -> list[WorkflowRun]: ...

    @abc.abstractmethod
    async def find_by_resource_id(self, vendor_id: str) -> list[WorkflowRun]: ...

    @abc.abstractmethod
    async def find_by_engine_run_id(self, engine_run_id: str) -> list[WorkflowRun]:
        """Runs whose engine run id (the string trigger_workflow returned) matches — the lookup a
        workflow-engine completion callback uses to resolve which run to wake."""

    @abc.abstractmethod
    async def list_recent(self, limit: int = 200) -> list[WorkflowRun]:
        """The most recently created runs, newest first (capped by ``limit``)."""


class WorkflowRepository(abc.ABC):
    """Registration + lookup for the workflow registry."""

    @abc.abstractmethod
    async def register(self, workflow: Workflow) -> Workflow: ...

    @abc.abstractmethod
    async def get_by_identifier(self, identifier: str) -> Workflow | None: ...

    @abc.abstractmethod
    async def update(self, workflow: Workflow) -> Workflow | None:
        """Overwrite the mutable fields of the registration keyed by ``workflow.identifier``
        (identifier is immutable; workflow_id and created_at are preserved). Returns the
        updated entity, or None if no workflow has that identifier."""

    @abc.abstractmethod
    async def list(self) -> list[Workflow]: ...


class TicketSystemClient(abc.ABC):
    """Domain-level ticket operations (ServiceNow is one implementation)."""

    @abc.abstractmethod
    async def open_ticket(
        self,
        template_id: str,
        fields: dict[str, Any],
        requested_by: str,
        idempotency_key: str,
        approval_group: str | None = None,
    ) -> TicketRef:
        """Open a ticket from a provider template (idempotent on the key). ``approval_group`` names
        the team whose approval the request needs, and the ticket is assigned to them — the adapter
        resolves the name to whatever the provider needs; None leaves routing to the provider."""

    @abc.abstractmethod
    async def get_approval_status(self, ticket: TicketRef) -> ApprovalStatus:
        """Current approval state of the ticket (pending until a human approves/rejects)."""

    @abc.abstractmethod
    async def close_ticket(
        self,
        ticket: TicketRef,
        note: str | None = None,
        outcome: TicketOutcome = TicketOutcome.SUCCESSFUL,
    ) -> None:
        """Close the ticket, optionally attaching a note. ``outcome`` says whether the request was
        fulfilled — a run that failed closes its ticket UNSUCCESSFUL, so a failed request is not
        indistinguishable from a completed one."""

    @abc.abstractmethod
    async def annotate_ticket(self, ticket: TicketRef, note: str) -> None:
        """Attach a note to the ticket without changing its state."""

    @abc.abstractmethod
    async def open_incident(
        self,
        summary: str,
        requested_by: str,
        responsible_group: str,
        flow_type: str | None = None,
        failed_task: str | None = None,
        comment: str | None = None,
        description: str | None = None,
    ) -> TicketRef:
        """Raise an incident for a failure, routed to the responsible group.

        ``summary`` is the one-line title. ``description`` is the body a responder reads first —
        what failed and why. ``comment``, when given, is attached as a note; it carries the same
        failure detail, so the detail survives even where a provider treats the body as immutable.
        """


class ResourceManagerClient(abc.ABC):
    """Domain-level resource operations (Project Manager is one implementation)."""

    @abc.abstractmethod
    async def create_resource(
        self, project_id: str, resource_type: str, body: dict[str, Any], idempotency_key: str
    ) -> dict[str, Any]:
        """Create a project resource; body carries vendor_id/name/region/etc."""

    @abc.abstractmethod
    async def update_resource(
        self, project_id: str, resource_type: str, vendor_id: str, fields: dict[str, Any]
    ) -> None:
        """Partial update (only changed fields) — e.g. in_progress=False on finalize.
        (There is no delete endpoint in the provider; nothing is ever removed.)"""


class WorkflowEngineClient(abc.ABC):
    """Domain-level engine operations (Airflow is one implementation)."""

    @abc.abstractmethod
    async def trigger_workflow(
        self, automation_id: str, params: dict[str, Any], idempotency_key: str
    ) -> str:
        """Start a run (idempotent on the key) and return the engine's run id."""

    @abc.abstractmethod
    async def query_run_status(self, automation_id: str, run_id: str) -> EngineRunStatus: ...

    @abc.abstractmethod
    async def get_failure(self, automation_id: str, run_id: str) -> EngineFailure:
        """Return typed failure detail for a failed run (task, responsible group, message)."""

    @abc.abstractmethod
    async def get_output(self, automation_id: str, run_id: str, key: str) -> str | None:
        """Return a named output value the run produced (Airflow: an XCom), or None if the
        run produced no output under that key. Best-effort enrichment — not every run has one."""
