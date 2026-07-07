*[← Index](README.md)*

# Stores

Two ports, two adapters — the run store and the workflow registry, both over one Postgres. There is
**no separate queue**: anything deferred (poll, retry backoff) is expressed as
`WorkflowRun.scheduled_at`, and the workers claim due rows and drive them directly (see
[02-architecture](02-architecture.md)). `claim_due` + the re-drive lease already give at-least-once
and crash recovery, so a queue transport would only duplicate them.

## `ports.py` — the storage contracts

```python
import abc
import uuid

from orchestrator.domain import Workflow, WorkflowRun


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
    async def find_by_ticket_id(self, ticket_id: str) -> list[WorkflowRun]: ...

    @abc.abstractmethod
    async def find_by_resource_id(self, vendor_id: str) -> list[WorkflowRun]: ...


class WorkflowRepository(abc.ABC):
    """Registration + lookup for the workflow registry."""

    @abc.abstractmethod
    async def register(self, workflow: Workflow) -> Workflow: ...

    @abc.abstractmethod
    async def get_by_identifier(self, identifier: str) -> Workflow | None: ...

    @abc.abstractmethod
    async def list(self) -> list[Workflow]: ...
```

## `adapters/database.py` — Postgres repositories

`expire_on_commit=False` + `expunge()` keep returned entities usable after the session closes (no
detached-instance crashes). `save()` re-reads the row `FOR UPDATE`, checks the version, and writes
every mutable field back — including the whole `state` model (no in-place tracking needed).

```python
import uuid
from datetime import timedelta

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlmodel import select

from orchestrator.domain import StaleRunError, Workflow, WorkflowRun, utcnow
from orchestrator.ports import WorkflowRepository, WorkflowRunRepository


def make_session_factory(database_url: str) -> async_sessionmaker[AsyncSession]:
    engine = create_async_engine(database_url)
    return async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


class PostgresWorkflowRunRepository(WorkflowRunRepository):
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self.session_factory = session_factory

    async def create(self, run: WorkflowRun) -> WorkflowRun:
        async with self.session_factory() as session:
            async with session.begin():
                session.add(run)
            session.expunge(run)
            return run

    async def get(self, run_id: uuid.UUID) -> WorkflowRun | None:
        async with self.session_factory() as session:
            run = await session.get(WorkflowRun, run_id)
            if run is not None:
                session.expunge(run)
            return run

    async def save(self, run: WorkflowRun) -> None:
        async with self.session_factory() as session:
            async with session.begin():
                db = await session.get(WorkflowRun, run.run_id, with_for_update=True)
                if db is None:
                    raise StaleRunError(f"Run {run.run_id} no longer exists")
                if db.version != run.version:
                    raise StaleRunError(f"Run {run.run_id} changed concurrently "
                                        f"(db v{db.version} != v{run.version})")
                db.status = run.status
                db.current_step = run.current_step
                db.scheduled_at = run.scheduled_at   # future time (poll/retry) or NULL (terminal)
                db.run_state = run.run_state.model_copy(deep=True)  # whole-model write
                db.updated_at = utcnow()
                db.version = run.version + 1
            run.version = db.version                 # keep the entity usable for a later save

    async def claim_due(self, limit: int, lease_seconds: float) -> list[uuid.UUID]:
        async with self.session_factory() as session:
            async with session.begin():
                now = utcnow()
                stmt = (
                    select(WorkflowRun)
                    .where(WorkflowRun.scheduled_at.is_not(None), WorkflowRun.scheduled_at <= now)
                    .order_by(WorkflowRun.scheduled_at.asc())
                    .with_for_update(skip_locked=True)
                    .limit(limit)
                )
                rows = (await session.execute(stmt)).scalars().all()
                ids: list[uuid.UUID] = []
                for run in rows:                     # push forward: if we/the worker die, it re-drives
                    run.scheduled_at = now + timedelta(seconds=lease_seconds)
                    run.updated_at = now
                    ids.append(run.run_id)
            return ids

    async def find_by_ticket_id(self, ticket_id: str) -> list[WorkflowRun]:
        return await self._find_by_path(("ticket", "ticket_id"), ticket_id)

    async def find_by_resource_id(self, vendor_id: str) -> list[WorkflowRun]:
        return await self._find_by_path(("resource", "vendor_id"), vendor_id)

    async def _find_by_path(self, path: tuple[str, str], value: str) -> list[WorkflowRun]:
        async with self.session_factory() as session:
            col = WorkflowRun.run_state
            stmt = select(WorkflowRun).where(col[path[0]][path[1]].astext == value)
            runs = list((await session.execute(stmt)).scalars().all())
            for r in runs:
                session.expunge(r)
            return runs


class PostgresWorkflowRepository(WorkflowRepository):
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self.session_factory = session_factory

    async def register(self, workflow: Workflow) -> Workflow:
        async with self.session_factory() as session:
            async with session.begin():
                session.add(workflow)
            session.expunge(workflow)
            return workflow

    async def get_by_identifier(self, identifier: str) -> Workflow | None:
        async with self.session_factory() as session:
            wf = (await session.execute(
                select(Workflow).where(Workflow.identifier == identifier)
            )).scalars().first()
            if wf is not None:
                session.expunge(wf)
            return wf

    async def list(self) -> list[Workflow]:
        async with self.session_factory() as session:
            rows = list((await session.execute(select(Workflow))).scalars().all())
            for wf in rows:
                session.expunge(wf)
            return rows
```

There is no queue adapter: each `RunWorker` ([08-entrypoints](08-entrypoints.md)) calls
`claim_due(1)` and drives the returned run through `RunExecutor.handle`, then loops. The
`workflow_runs` table ordered by `scheduled_at` with `FOR UPDATE SKIP LOCKED` **is** the durable,
single-delivery work queue; a second transport would restate it.

## What got simpler here (vs. the previous revision)

- **The queue is gone** — `PgQueuerRunQueue`, the `RunQueue`/`RunHandler` port, the second asyncpg
  pool, and the `pgq install` step are all cut. `claim_due` + the re-drive lease already provided
  at-least-once and crash recovery, so pgqueuer was a 1:1 duplication of the workers' own claim
  loop; dropping it removes a dependency and a whole delivery mechanism with no loss of guarantees.

- **`create(run)` / `register(workflow)` take a constructed entity**, not `**fields` — typed at
  the call site, mypy-checked, nothing stringly.
- **`find_by_state(key, value)` became explicit finders** (`find_by_ticket_id`,
  `find_by_resource_id`, `find_by_engine_run_id`) matching the API filters and the callback
  lookups that actually exist. A generic key/value query invited untyped state coupling.
- **`record_callback` never came back** — the wake-early callback does not record anything on the
  run; it calls `wake(run_id)`, a targeted `scheduled_at=now` UPDATE *outside* the optimistic
  `version` scheme (it never raises `StaleRunError`), and the authoritative status still comes from
  the poll (see [01-external-contracts](01-external-contracts.md)).
- **No mapper layer** — the repositories read and write `WorkflowRun`/`Workflow` directly
  (they're SQLModel); `state` round-trips through `PydanticJSONB` automatically.
