*[← Index](README.md)*

# Domain & Config

Everything here is **typed**: settings via pydantic-settings, entities via SQLModel (one class =
Pydantic model *and* table), and the run's working state via an explicit `RunState` model — no
untyped dicts with magic keys.

## `config.py` — Settings (pydantic-settings)

One database DSN — the run store and the workflow registry live in the same Postgres. There is no
separate queue, so there is no second DSN to derive.

```python
from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="ORCH_", env_file=".env", extra="ignore")

    # Single Postgres DSN (SQLAlchemy async form), e.g. postgresql+asyncpg://user:pw@host/db.
    # The run store and the workflow registry both live in this database.
    database_url: str

    # Logging: threshold for the stdout sink installed by log.configure_logging().
    # DEBUG/INFO/WARNING/ERROR/CRITICAL; applied after the log config document, so it always
    # wins. An unrecognised name degrades to INFO with a warning rather than failing startup.
    log_level: str = "INFO"

    # Retry policy (application-owned; expressed as WorkflowRun.scheduled_at, never a queue delay).
    # Poll intervals live on the step handlers themselves (StepHandler.poll_interval_seconds).
    retry_base_seconds: float = 10.0             # base for per-step exponential backoff

    # Workers (each loop claims one due run and drives it; the pool size sets concurrency)
    worker_concurrency_limit: int = 16           # RunWorker loops per daemon
    worker_poll_interval_seconds: float = 1.0    # idle re-check interval per worker
    redrive_lease_seconds: float = 300.0         # re-drive lease: a claimed run reappears if its
                                                 # processing never completes (crash recovery).

    # Airflow (first workflow engine) — REST API v2, token auth, verify=False per spec
    airflow_base_url: str
    airflow_username: str
    airflow_password: SecretStr

    # ServiceNow (first ticket system)
    servicenow_base_url: str
    servicenow_username: str
    servicenow_password: SecretStr
    # team-name -> full ServiceNow assignment-group name; unknown names are used verbatim.
    servicenow_responsible_groups: dict[str, str] = Field(default_factory=dict)
    servicenow_incident_team: str = "cloudio"    # default team for failure incidents

    # Project Manager (first resource manager)
    pm_base_url: str
    pm_token: SecretStr

    external_call_timeout_seconds: float = 10.0
```

## `domain.py` — enums, value objects, typed run state, entities

```python
import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field as PyField
from sqlalchemy import Column, DateTime, Integer, String, types
from sqlalchemy import Enum as SQLEnum
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PgUUID
from sqlmodel import Field, SQLModel


def utcnow() -> datetime:
    """Timezone-aware UTC. Never persist naive datetimes into TIMESTAMPTZ."""
    return datetime.now(timezone.utc)


# --- Enums ---

class RunType(str, Enum):
    AUTOMATION = "automation"
    RESOURCE = "resource"


class RunStatus(str, Enum):
    PENDING = "pending"       # created, not yet started
    RUNNING = "running"       # being driven, or waiting on a scheduled poll/retry (scheduled_at)
    COMPLETED = "completed"   # terminal: all steps done
    FAILED = "failed"         # terminal: a step exhausted its retries (no rollback)


class WorkflowEngineType(str, Enum):
    AIRFLOW = "airflow"       # first engine; extend here (e.g. TEMPORAL = "temporal")


class StepName(str, Enum):
    CREATE_TICKET = "creating_ticket"
    CONFIGURE_RESOURCE = "configuring_resource"       # create/mark-in-progress the resource
    RUN_ENGINE = "running_engine"
    FINALIZE_RESOURCE = "finalizing_resource"   # mark the resource operation done
    CLOSE_TICKET = "closing_ticket"             # close out the RITM


class EngineRunStatus(str, Enum):
    SUCCESS = "success"
    FAILED = "failed"
    IN_PROGRESS = "in_progress"


# --- Exceptions ---

class StaleRunError(Exception):
    """A save targeted a run version that changed underneath us (an overlapping re-drive).
    The executor drops the save; a worker re-drives the run."""


class StepDeadlineExceeded(Exception):
    """A step ran past its overall wall-clock budget across all polls."""


class UnknownWorkflowError(Exception):
    """Trigger named a workflow identifier that is not registered."""


class ResourceParamsRequired(Exception):
    """A resource workflow was triggered without a resource spec."""


# --- Value objects (pure Pydantic) ---

class TicketRef(BaseModel):
    """A ticket-system record reference; BOTH identifiers are persisted.
    ServiceNow: ticket_id == RITM number (e.g. RITM0012345), native_id == sys_id."""
    ticket_id: str
    native_id: str


class ResourceOperation(str, Enum):
    CREATE = "create"         # provision a new record (the run id becomes its vendor id)
    UPDATE = "update"         # act on an existing record
    DELETE = "delete"         # act on an existing record


class ResourceSpec(BaseModel):
    """The resource a resource run acts on (Project Manager fields)."""
    project_id: str
    resource_type: str
    operation: ResourceOperation = ResourceOperation.CREATE
    vendor_id: str            # resource identity; a CREATE is assigned the run id when configured
    name: str
    region: str
    environment: str
    description: str = ""
    tags: list[str] = PyField(default_factory=list)
    data: dict[str, Any] = PyField(default_factory=dict)
    alert_groups: list[str] = PyField(default_factory=list)


class ResolvedWorkflow(BaseModel):
    """The registry mapping, snapshotted into the run at trigger time."""
    identifier: str
    engine_type: WorkflowEngineType
    automation_id: str        # Airflow: DAG id
    ticket_template_id: str   # provider ticket template (ServiceNow: catalog item sys_id)


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
    ticket_params: dict[str, Any] = PyField(default_factory=dict)    # provider template variables (pass-through)
    workflow_params: dict[str, Any] = PyField(default_factory=dict)  # engine conf (pass-through)
    resource: ResourceSpec | None = None                             # resource runs only

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

class PydanticJSONB(types.TypeDecorator):
    """Store a Pydantic model in a JSONB column: model_dump on save, model_validate on load.
    This one helper replaces the previous entity⇄table mapping layer AND MutableDict tracking —
    the repository writes the whole value back on save()."""
    impl = JSONB
    cache_ok = True

    def __init__(self, model: type[BaseModel]) -> None:
        super().__init__()
        self.model = model

    def process_bind_param(self, value: BaseModel | None, dialect: object) -> dict | None:
        return value.model_dump(mode="json") if value is not None else None

    def process_result_value(self, value: dict | None, dialect: object) -> BaseModel | None:
        return self.model.model_validate(value) if value is not None else None


def _enum_values(enum_cls: type[Enum]) -> list[str]:
    # Persist the enum VALUE (lowercase) to match the native PG ENUM labels, not the member NAME.
    return [m.value for m in enum_cls]


# --- Entities (SQLModel: Pydantic model AND table in one class) ---

class WorkflowRun(SQLModel, table=True):
    """The durable run instance. Owns step progress + idempotency state in `run_state` (typed).
    Holds NO queue mechanics; `scheduled_at` is application scheduling — "(re-)drive me
    at/after this time" — not a lease."""
    __tablename__ = "workflow_runs"

    run_id: uuid.UUID = Field(default_factory=uuid.uuid4,
                              sa_column=Column(PgUUID(as_uuid=True), primary_key=True))
    run_type: RunType = Field(sa_column=Column(
        SQLEnum(RunType, name="run_type", values_callable=_enum_values, create_type=False),
        nullable=False))
    status: RunStatus = Field(default=RunStatus.PENDING, sa_column=Column(
        SQLEnum(RunStatus, name="run_status", values_callable=_enum_values, create_type=False),
        nullable=False))
    # str column (not a native enum) so the step list can evolve without a migration;
    # StepName is a str-enum, so comparisons against it Just Work.
    current_step: StepName | None = Field(default=None, sa_column=Column(String(64), nullable=True))
    workflow_identifier: str = Field(sa_column=Column(String(255), nullable=False, index=True))
    created_by: str = Field(sa_column=Column(String(255), nullable=False))
    max_retries: int = Field(default=3, sa_column=Column(Integer, nullable=False))

    # Application-owned scheduling clock: the workers claim due rows and drive them; the
    # executor sets a future time to poll/retry, or NULL when terminal / in-flight.
    scheduled_at: datetime | None = Field(default_factory=utcnow,
                                          sa_column=Column(DateTime(timezone=True), nullable=True))
    created_at: datetime = Field(default_factory=utcnow,
                                 sa_column=Column(DateTime(timezone=True), nullable=False))
    updated_at: datetime = Field(default_factory=utcnow,
                                 sa_column=Column(DateTime(timezone=True), nullable=False))

    # Optimistic concurrency: save() checks this, bumps it, raises StaleRunError on conflict.
    version: int = Field(default=1, sa_column=Column(Integer, nullable=False))

    # The typed working state — one JSONB column, validated on load.
    run_state: RunState = Field(sa_column=Column(PydanticJSONB(RunState), nullable=False))


class Workflow(SQLModel, table=True):
    """Registry mapping a stable identifier to how a workflow run is built and routed."""
    __tablename__ = "workflows"

    workflow_id: uuid.UUID = Field(default_factory=uuid.uuid4,
                                   sa_column=Column(PgUUID(as_uuid=True), primary_key=True))
    identifier: str = Field(sa_column=Column(String(255), unique=True, nullable=False, index=True))
    name: str | None = Field(default=None, sa_column=Column(String(255), nullable=True))
    run_type: RunType = Field(sa_column=Column(
        SQLEnum(RunType, name="run_type", values_callable=_enum_values, create_type=False),
        nullable=False))
    engine_type: WorkflowEngineType = Field(sa_column=Column(
        SQLEnum(WorkflowEngineType, name="workflow_engine_type", values_callable=_enum_values,
                create_type=False), nullable=False))
    automation_id: str = Field(sa_column=Column(String(255), nullable=False))
    ticket_template_id: str = Field(sa_column=Column(String(255), nullable=False))
    created_at: datetime = Field(default_factory=utcnow,
                                 sa_column=Column(DateTime(timezone=True), nullable=False))
```

## DDL beyond `create_all` (Alembic `0001_initial.py`)

```sql
CREATE TYPE run_type AS ENUM ('automation', 'resource');
CREATE TYPE run_status AS ENUM ('pending','running','completed','failed');
CREATE TYPE workflow_engine_type AS ENUM ('airflow');

-- (workflow_runs and workflows tables are generated from the SQLModel metadata;
--  workflows.identifier carries a UNIQUE index, workflow_runs.workflow_identifier a plain index)

-- Worker claim index: the RunWorkers claim due runs by this. Lead on scheduled_at for the
-- ORDER BY; the partial predicate keeps terminal/in-flight (NULL) rows out of the index.
CREATE INDEX idx_runs_scheduled_at ON workflow_runs (scheduled_at)
    WHERE scheduled_at IS NOT NULL;

-- Lookup indexes for the list-by-ticket / list-by-resource API (typed JSONB paths).
CREATE INDEX idx_runs_ticket_id ON workflow_runs ((run_state #>> '{ticket,ticket_id}'))
    WHERE (run_state #>> '{ticket,ticket_id}') IS NOT NULL;
CREATE INDEX idx_runs_resource_vendor_id ON workflow_runs ((run_state #>> '{resource,vendor_id}'))
    WHERE (run_state #>> '{resource,vendor_id}') IS NOT NULL;

-- Retention: periodically archive/purge terminal workflow_runs rows.
```

## What got simpler here (vs. the previous revision)

- **One `WorkflowRun` class** instead of pure-entity + `WorkflowRunTable` + two mappers. SQLModel
  *is* the Pydantic/ORM unification; using it twice was over-engineering.
- **`RunState` replaces the `dict[str, Any]` state** and its ~15 magic string keys
  (`ticket_sys_id`, `running_automation_engine_completed`, `steps.finalize_resource`, …).
  Handlers now read/write typed attributes; mypy catches what used to be runtime KeyErrors.
- **`MutableDict` is gone** — `PydanticJSONB` + whole-value writes on `save()` need no in-place
  change tracking.
- **One DSN and no queue** — a single `database_url`; the workers claim and drive runs straight
  from the run store, so there is no second (queue) DSN or connection pool to configure.
- **No webhook settings** (`public_base_url`, `webhook_secret`) — the wake-early callback endpoints
  are network-trust (no app-layer auth) and inbound-only, so they need no config; see
  [01-external-contracts](01-external-contracts.md).

The HTTP request/response schemas live with the API in [08-entrypoints](08-entrypoints.md).
