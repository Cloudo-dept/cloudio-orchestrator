"""In-memory implementations of the ports for fast, DB-free unit tests.

These fakes mirror the *observable contract* of the real adapters (idempotency on keys, optimistic
version checks, due-claim + lease) without any I/O — they are the substitution point for unit tests.
"""

import uuid
from typing import Any

from orchestrator.domain import (
    ApprovalStatus,
    EngineFailure,
    EngineRunStatus,
    RunStatus,
    StaleRunError,
    TicketOutcome,
    TicketRef,
    Workflow,
    WorkflowAlreadyExistsError,
    WorkflowRun,
    utcnow,
)
from orchestrator.ports import (
    HealthCheck,
    ResourceManagerClient,
    TicketSystemClient,
    WorkflowEngineClient,
    WorkflowRepository,
    WorkflowRunRepository,
)

_TERMINAL_STATUSES = (RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.REJECTED)


class FakeHealthCheck(HealthCheck):
    """Toggle ``healthy`` to simulate the backing dependency being reachable or down."""

    def __init__(self, healthy: bool = True) -> None:
        self.healthy = healthy

    async def ping(self) -> None:
        if not self.healthy:
            raise RuntimeError("dependency unreachable")


def _copy(run: WorkflowRun) -> WorkflowRun:
    """Independent deep copy — callers mutate their own copy; save() writes it back."""
    return run.model_copy(deep=True)


class FakeWorkflowRunRepository(WorkflowRunRepository):
    def __init__(self) -> None:
        self._runs: dict[uuid.UUID, WorkflowRun] = {}

    async def create(self, run: WorkflowRun) -> WorkflowRun:
        self._runs[run.run_id] = _copy(run)
        return _copy(run)

    async def get(self, run_id: uuid.UUID) -> WorkflowRun | None:
        stored = self._runs.get(run_id)
        return _copy(stored) if stored is not None else None

    async def save(self, run: WorkflowRun) -> None:
        stored = self._runs.get(run.run_id)
        if stored is None:
            raise StaleRunError(f"Run {run.run_id} no longer exists")
        if stored.version != run.version:
            raise StaleRunError(
                f"Run {run.run_id} changed concurrently (store v{stored.version} != v{run.version})"
            )
        new_version = run.version + 1
        saved = _copy(run)
        saved.version = new_version
        saved.updated_at = utcnow()
        self._runs[run.run_id] = saved
        run.version = new_version  # keep the caller's entity usable for a later save

    async def claim_due(self, limit: int, lease_seconds: float) -> list[uuid.UUID]:
        from datetime import timedelta

        now = utcnow()
        due = sorted(
            (
                r
                for r in self._runs.values()
                if r.scheduled_at is not None and r.scheduled_at <= now
            ),
            key=lambda r: r.scheduled_at,  # type: ignore[arg-type,return-value]
        )[:limit]
        claimed: list[uuid.UUID] = []
        for run in due:
            run.scheduled_at = now + timedelta(seconds=lease_seconds)
            run.updated_at = now
            claimed.append(run.run_id)
        return claimed

    async def find_by_ticket_id(self, ticket_id: str) -> list[WorkflowRun]:
        return [
            _copy(r)
            for r in self._runs.values()
            if r.run_state.ticket is not None and r.run_state.ticket.ticket_id == ticket_id
        ]

    async def list_recent(self, limit: int = 200) -> list[WorkflowRun]:
        newest_first = sorted(self._runs.values(), key=lambda r: r.created_at, reverse=True)
        return [_copy(r) for r in newest_first[:limit]]

    async def find_by_resource_id(self, vendor_id: str) -> list[WorkflowRun]:
        return [
            _copy(r)
            for r in self._runs.values()
            if r.run_state.resource is not None and r.run_state.resource.vendor_id == vendor_id
        ]

    async def find_by_engine_run_id(self, engine_run_id: str) -> list[WorkflowRun]:
        return [
            _copy(r)
            for r in self._runs.values()
            if r.run_state.engine_run_id == engine_run_id
        ]

    async def wake(self, run_id: uuid.UUID) -> bool:
        # A nudge outside the version scheme: make a non-terminal run due now, no version bump.
        stored = self._runs.get(run_id)
        if stored is None or stored.status in _TERMINAL_STATUSES:
            return False
        stored.scheduled_at = utcnow()
        stored.updated_at = utcnow()
        return True


class FakeWorkflowRepository(WorkflowRepository):
    def __init__(self) -> None:
        self._by_identifier: dict[str, Workflow] = {}

    async def register(self, workflow: Workflow) -> Workflow:
        if workflow.identifier in self._by_identifier:
            raise WorkflowAlreadyExistsError(workflow.identifier)
        self._by_identifier[workflow.identifier] = workflow.model_copy(deep=True)
        return workflow.model_copy(deep=True)

    async def get_by_identifier(self, identifier: str) -> Workflow | None:
        wf = self._by_identifier.get(identifier)
        return wf.model_copy(deep=True) if wf is not None else None

    async def update(self, workflow: Workflow) -> Workflow | None:
        existing = self._by_identifier.get(workflow.identifier)
        if existing is None:
            return None
        existing.name = workflow.name
        existing.run_type = workflow.run_type
        existing.engine_type = workflow.engine_type
        existing.automation_id = workflow.automation_id
        existing.ticket_template_id = workflow.ticket_template_id
        return existing.model_copy(deep=True)

    async def list(self) -> list[Workflow]:
        return [wf.model_copy(deep=True) for wf in self._by_identifier.values()]


class FakeTicketSystemClient(TicketSystemClient):
    def __init__(self) -> None:
        self.tickets_by_key: dict[str, TicketRef] = {}
        self.open_ticket_calls: list[str] = []  # idempotency keys, in order
        self.opened_fields: list[dict[str, Any]] = []  # template variables per open_ticket call
        self.closed: list[tuple[str, str | None, TicketOutcome]] = []  # (ticket_id, note, outcome)
        self.notes: list[tuple[str, str]] = []  # (ticket_id, note)
        self.incidents: list[dict[str, Any]] = []
        # default APPROVED so a resource run drives straight through AWAIT_APPROVAL; flip to
        # PENDING/REJECTED to exercise the wait/rejection paths.
        self.approval_status = ApprovalStatus.APPROVED
        self._seq = 0

    def _mint(self, prefix: str) -> TicketRef:
        self._seq += 1
        return TicketRef(ticket_id=f"{prefix}{self._seq:07d}", native_id=f"sys{self._seq:07d}")

    async def open_ticket(
        self,
        template_id: str,
        fields: dict[str, Any],
        requested_by: str,
        idempotency_key: str,
    ) -> TicketRef:
        self.open_ticket_calls.append(idempotency_key)
        self.opened_fields.append(fields)
        if idempotency_key in self.tickets_by_key:  # idempotent on the key
            return self.tickets_by_key[idempotency_key]
        ref = self._mint("RITM")
        self.tickets_by_key[idempotency_key] = ref
        return ref

    async def get_approval_status(self, ticket: TicketRef) -> ApprovalStatus:
        return self.approval_status

    async def close_ticket(
        self,
        ticket: TicketRef,
        note: str | None = None,
        outcome: TicketOutcome = TicketOutcome.SUCCESSFUL,
    ) -> None:
        self.closed.append((ticket.ticket_id, note, outcome))

    async def annotate_ticket(self, ticket: TicketRef, note: str) -> None:
        self.notes.append((ticket.ticket_id, note))

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
        ref = self._mint("INC")
        self.incidents.append(
            {
                "ticket_id": ref.ticket_id,
                "summary": summary,
                "requested_by": requested_by,
                "responsible_group": responsible_group,
                "flow_type": flow_type,
                "failed_task": failed_task,
                "comment": comment,
                "description": description,
            }
        )
        return ref


class FakeResourceManagerClient(ResourceManagerClient):
    def __init__(self) -> None:
        self.created_by_key: dict[str, dict[str, Any]] = {}
        self.create_calls: list[str] = []  # idempotency keys, in order
        self.updated: list[tuple[str, str, str, dict[str, Any]]] = []

    async def create_resource(
        self, project_id: str, resource_type: str, body: dict[str, Any], idempotency_key: str
    ) -> dict[str, Any]:
        self.create_calls.append(idempotency_key)
        if idempotency_key in self.created_by_key:  # idempotent on the key
            return self.created_by_key[idempotency_key]
        resource = {**body, "in_progress": True}
        self.created_by_key[idempotency_key] = resource
        return resource

    async def update_resource(
        self, project_id: str, resource_type: str, vendor_id: str, fields: dict[str, Any]
    ) -> None:
        self.updated.append((project_id, resource_type, vendor_id, fields))


class FakeWorkflowEngineClient(WorkflowEngineClient):
    def __init__(self, status: EngineRunStatus = EngineRunStatus.SUCCESS) -> None:
        self.status = status  # flip between drives to model polling
        self.failure = EngineFailure()
        self.outputs: dict[str, str] = {}  # named run outputs (Airflow: XComs), by key
        self.triggered_by_key: dict[str, str] = {}
        self.trigger_calls: list[tuple[str, str]] = []  # (automation_id, idempotency_key)
        self._seq = 0

    async def trigger_workflow(
        self, automation_id: str, params: dict[str, Any], idempotency_key: str
    ) -> str:
        self.trigger_calls.append((automation_id, idempotency_key))
        if idempotency_key in self.triggered_by_key:  # idempotent on the key
            return self.triggered_by_key[idempotency_key]
        self._seq += 1
        run_id = f"dagrun-{self._seq}"
        self.triggered_by_key[idempotency_key] = run_id
        return run_id

    async def query_run_status(self, automation_id: str, run_id: str) -> EngineRunStatus:
        return self.status

    async def get_failure(self, automation_id: str, run_id: str) -> EngineFailure:
        return self.failure

    async def get_output(self, automation_id: str, run_id: str, key: str) -> str | None:
        return self.outputs.get(key)
