"""Enums, value objects, typed run state, and entities.

Everything here is typed: entities via SQLModel (one class = Pydantic model *and* table), the run's
working state via an explicit ``RunState`` model. No untyped dicts with magic keys.
"""

import uuid
from datetime import UTC, datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel
from pydantic import Field as PyField
from sqlalchemy import Column, DateTime, Integer, String, types
from sqlalchemy import Enum as SQLEnum
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PgUUID
from sqlmodel import Field, SQLModel


def utcnow() -> datetime:
    """Timezone-aware UTC. Never persist naive datetimes into TIMESTAMPTZ."""
    return datetime.now(UTC)


# --- Enums ---


class RunType(str, Enum):
    AUTOMATION = "automation"
    RESOURCE = "resource"


class RunStatus(str, Enum):
    PENDING = "pending"  # created, not yet started
    RUNNING = "running"  # being driven, or waiting on a scheduled poll/retry (scheduled_at)
    COMPLETED = "completed"  # terminal: all steps done
    FAILED = "failed"  # terminal: a step exhausted its retries (no rollback)
    REJECTED = "rejected"  # terminal: request denied in the ticket system (no rollback/incident)


class WorkflowEngineType(str, Enum):
    AIRFLOW = "airflow"  # first engine; extend here (e.g. TEMPORAL = "temporal")


class StepName(str, Enum):
    CREATE_TICKET = "creating_ticket"
    AWAIT_APPROVAL = "awaiting_approval"  # wait for the ticket to be approved before provisioning
    # create the record, or mark an existing one in-progress, for the resource operation
    CONFIGURE_RESOURCE = "configuring_resource"
    RUN_ENGINE = "running_engine"
    FINALIZE_RESOURCE = "finalizing_resource"  # mark the resource operation done
    CLOSE_TICKET = "closing_ticket"  # close out the RITM


class ResourceOperation(str, Enum):
    """What a resource run does to its resource. A CREATE provisions a new record; UPDATE and
    DELETE act on a record that already exists (the engine does the real work either way)."""

    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"


class EngineRunStatus(str, Enum):
    SUCCESS = "success"
    FAILED = "failed"
    IN_PROGRESS = "in_progress"


class ApprovalStatus(str, Enum):
    """Ticket approval state, provider-neutral. PENDING until a human approves or rejects."""

    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"


# --- Exceptions ---


class StaleRunError(Exception):
    """A save targeted a run version that changed underneath us (an overlapping re-drive).
    The executor drops the save; a worker re-drives the run."""


class StepDeadlineExceeded(Exception):
    """A step ran past its overall wall-clock budget across all polls."""


class RunRejected(Exception):
    """A gating step reported the request was denied; the run stops terminally
    without retry or escalation."""


class UnknownWorkflowError(Exception):
    """Trigger named a workflow identifier that is not registered."""


class WorkflowAlreadyExistsError(Exception):
    """Register was called with an identifier that is already registered."""


class ResourceParamsRequired(Exception):
    """A resource workflow was triggered without a resource spec."""


# --- Value objects (pure Pydantic) ---


class TicketRef(BaseModel):
    """A ticket-system record reference; BOTH identifiers are persisted.
    ServiceNow: ticket_id == RITM number (e.g. RITM0012345), native_id == sys_id."""

    ticket_id: str
    native_id: str


class ResourceSpec(BaseModel):
    """The resource a resource run acts on (Project Manager fields)."""

    project_id: str
    resource_type: str
    operation: ResourceOperation = ResourceOperation.CREATE  # create / update / delete
    # Resource identity. Callers do NOT set this for a CREATE — ConfigureResourceStep assigns the
    # run id as the new record's identity. Supplied by the caller only to target an existing
    # record for an UPDATE/DELETE.
    vendor_id: str = ""
    name: str
    region: str | None = None
    environment: str | None = None
    description: str = ""
    tags: list[str] = PyField(default_factory=list)
    data: dict[str, Any] = PyField(default_factory=dict)
    alert_groups: list[str] = PyField(default_factory=list)


class ResolvedWorkflow(BaseModel):
    """The registry mapping, snapshotted into the run at trigger time."""

    identifier: str
    engine_type: WorkflowEngineType
    automation_id: str  # Airflow: DAG id
    ticket_template_id: str  # provider ticket template (ServiceNow: catalog item sys_id)


class EngineFailure(BaseModel):
    """What the engine reported about a failed run (drives incident routing)."""

    failed_task: str | None = None
    responsible_group: str | None = None
    detail: str | None = None


class RunState(BaseModel):
    """The run's entire working state — explicit schema, persisted as ONE JSONB column.
    Step handlers read/write typed fields; the booleans are the idempotency markers that
    make re-drives safe."""

    workflow: ResolvedWorkflow
    ticket_params: dict[str, Any] = PyField(default_factory=dict)  # provider template variables
    workflow_params: dict[str, Any] = PyField(default_factory=dict)  # engine conf (pass-through)
    resource: ResourceSpec | None = None  # resource runs only

    # step progress / idempotency markers
    ticket: TicketRef | None = None
    engine_run_id: str | None = None
    resource_configured: bool = False
    resource_finalized: bool = False
    ticket_closed: bool = False

    # retry / deadline bookkeeping (keyed by StepName)
    step_attempts: dict[StepName, int] = PyField(default_factory=dict)
    step_started_at: dict[StepName, datetime] = PyField(default_factory=dict)
    errors: dict[StepName, str] = PyField(default_factory=dict)

    # failure escalation
    engine_failure: EngineFailure | None = None
    incident_id: str | None = None


# --- Persistence helpers ---


class PydanticJSONB(types.TypeDecorator[Any]):
    """Store a Pydantic model in a JSONB column: model_dump on save, model_validate on load.
    The repository writes the whole value back on save()."""

    impl = JSONB
    cache_ok = True

    def __init__(self, model: type[BaseModel]) -> None:
        super().__init__()
        self.model = model

    def process_bind_param(self, value: Any, dialect: object) -> dict[str, Any] | None:
        if value is None:
            return None
        assert isinstance(value, BaseModel)
        return value.model_dump(mode="json")

    def process_result_value(self, value: Any, dialect: object) -> BaseModel | None:
        return self.model.model_validate(value) if value is not None else None


def _enum_values(enum_cls: type[Enum]) -> list[str]:
    # Persist the enum VALUE (lowercase) to match the native PG ENUM labels, not the member NAME.
    return [str(m.value) for m in enum_cls]


# --- Entities (SQLModel: Pydantic model AND table in one class) ---


class WorkflowRun(SQLModel, table=True):
    """The durable run instance. Owns step progress + idempotency state in ``run_state`` (typed).
    Holds NO queue mechanics; ``scheduled_at`` is application scheduling — "(re-)drive me
    at/after this time" — not a lease."""

    __tablename__ = "workflow_runs"

    run_id: uuid.UUID = Field(
        default_factory=uuid.uuid4, sa_column=Column(PgUUID(as_uuid=True), primary_key=True)
    )
    run_type: RunType = Field(
        sa_column=Column(
            SQLEnum(RunType, name="run_type", values_callable=_enum_values, create_type=False),
            nullable=False,
        )
    )
    status: RunStatus = Field(
        default=RunStatus.PENDING,
        sa_column=Column(
            SQLEnum(RunStatus, name="run_status", values_callable=_enum_values, create_type=False),
            nullable=False,
        ),
    )
    # str column (not a native enum) so the step list can evolve without a migration;
    # StepName is a str-enum, so comparisons against it Just Work.
    current_step: StepName | None = Field(default=None, sa_column=Column(String(64), nullable=True))
    workflow_identifier: str = Field(sa_column=Column(String(255), nullable=False, index=True))
    created_by: str = Field(sa_column=Column(String(255), nullable=False))
    max_retries: int = Field(default=3, sa_column=Column(Integer, nullable=False))

    # Application-owned scheduling clock: the workers claim due rows and drive them; the
    # executor sets a future time to poll/retry, or NULL when terminal / in-flight.
    scheduled_at: datetime | None = Field(
        default_factory=utcnow, sa_column=Column(DateTime(timezone=True), nullable=True)
    )
    created_at: datetime = Field(
        default_factory=utcnow, sa_column=Column(DateTime(timezone=True), nullable=False)
    )
    updated_at: datetime = Field(
        default_factory=utcnow, sa_column=Column(DateTime(timezone=True), nullable=False)
    )

    # Optimistic concurrency: save() checks this, bumps it, raises StaleRunError on conflict.
    version: int = Field(default=1, sa_column=Column(Integer, nullable=False))

    # The typed working state — one JSONB column, validated on load.
    run_state: RunState = Field(sa_column=Column(PydanticJSONB(RunState), nullable=False))


class Workflow(SQLModel, table=True):
    """Registry mapping a stable identifier to how a workflow run is built and routed."""

    __tablename__ = "workflows"

    workflow_id: uuid.UUID = Field(
        default_factory=uuid.uuid4, sa_column=Column(PgUUID(as_uuid=True), primary_key=True)
    )
    identifier: str = Field(sa_column=Column(String(255), unique=True, nullable=False, index=True))
    name: str | None = Field(default=None, sa_column=Column(String(255), nullable=True))
    run_type: RunType = Field(
        sa_column=Column(
            SQLEnum(RunType, name="run_type", values_callable=_enum_values, create_type=False),
            nullable=False,
        )
    )
    engine_type: WorkflowEngineType = Field(
        sa_column=Column(
            SQLEnum(
                WorkflowEngineType,
                name="workflow_engine_type",
                values_callable=_enum_values,
                create_type=False,
            ),
            nullable=False,
        )
    )
    automation_id: str = Field(sa_column=Column(String(255), nullable=False))
    ticket_template_id: str = Field(sa_column=Column(String(255), nullable=False))
    created_at: datetime = Field(
        default_factory=utcnow, sa_column=Column(DateTime(timezone=True), nullable=False)
    )
