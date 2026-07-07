"""Postgres repositories.

``expire_on_commit=False`` + ``expunge()`` keep returned entities usable after the session closes.
``save()`` re-reads the row FOR UPDATE, checks the version, and writes every mutable field back —
including the whole ``run_state`` model (no in-place tracking needed).
"""

import uuid
from datetime import timedelta

from sqlalchemy import String, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.sql import literal_column
from sqlmodel import select

from orchestrator.domain import (
    StaleRunError,
    Workflow,
    WorkflowAlreadyExistsError,
    WorkflowRun,
    utcnow,
)
from orchestrator.ports import HealthCheck, WorkflowRepository, WorkflowRunRepository


def make_session_factory(database_url: str) -> async_sessionmaker[AsyncSession]:
    engine = create_async_engine(database_url)
    return async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


class PostgresHealthCheck(HealthCheck):
    """Readiness probe for the run store: a round-trip ``SELECT 1`` on the pool."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self.session_factory = session_factory

    async def ping(self) -> None:
        async with self.session_factory() as session:
            await session.execute(text("SELECT 1"))


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
                    raise StaleRunError(
                        f"Run {run.run_id} changed concurrently "
                        f"(db v{db.version} != v{run.version})"
                    )
                db.status = run.status
                db.current_step = run.current_step
                db.scheduled_at = run.scheduled_at  # future time (poll/retry) or NULL (terminal)
                db.run_state = run.run_state.model_copy(deep=True)  # whole-model write
                db.updated_at = utcnow()
                db.version = run.version + 1
            run.version = db.version  # keep the entity usable for a later save

    async def claim_due(self, limit: int, lease_seconds: float) -> list[uuid.UUID]:
        async with self.session_factory() as session:
            async with session.begin():
                now = utcnow()
                stmt = (
                    select(WorkflowRun)
                    .where(
                        WorkflowRun.scheduled_at.is_not(None),  # type: ignore[union-attr]
                        WorkflowRun.scheduled_at <= now,  # type: ignore[operator]
                    )
                    .order_by(WorkflowRun.scheduled_at.asc())  # type: ignore[union-attr]
                    .with_for_update(skip_locked=True)
                    .limit(limit)
                )
                rows = (await session.execute(stmt)).scalars().all()
                ids: list[uuid.UUID] = []
                for run in rows:  # push forward -> re-drive on crash
                    run.scheduled_at = now + timedelta(seconds=lease_seconds)
                    run.updated_at = now
                    ids.append(run.run_id)
            return ids

    async def list_recent(self, limit: int = 200) -> list[WorkflowRun]:
        async with self.session_factory() as session:
            stmt = (
                select(WorkflowRun)
                .order_by(WorkflowRun.created_at.desc())  # type: ignore[attr-defined]
                .limit(limit)
            )
            rows = list((await session.execute(stmt)).scalars().all())
            for r in rows:
                session.expunge(r)
            return rows

    async def find_by_ticket_id(self, ticket_id: str) -> list[WorkflowRun]:
        return await self._find_by_path("{ticket,ticket_id}", ticket_id)

    async def find_by_resource_id(self, vendor_id: str) -> list[WorkflowRun]:
        return await self._find_by_path("{resource,vendor_id}", vendor_id)

    async def _find_by_path(self, json_path: str, value: str) -> list[WorkflowRun]:
        # `run_state #>> '{a,b}'` — matches the partial JSONB index in the migration.
        # `json_path` is a hardcoded constant (never user input), so inline it as a SQL literal:
        # a bound param would not match the constant-path expression index, and it also dodges
        # PydanticJSONB coercing the path/value operands. return_type=String binds `value` as text.
        async with self.session_factory() as session:
            column = WorkflowRun.run_state.op("#>>", return_type=String)(  # type: ignore[attr-defined]
                literal_column(f"'{json_path}'")
            )
            stmt = select(WorkflowRun).where(column == value)
            runs = list((await session.execute(stmt)).scalars().all())
            for r in runs:
                session.expunge(r)
            return runs


class PostgresWorkflowRepository(WorkflowRepository):
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self.session_factory = session_factory

    async def register(self, workflow: Workflow) -> Workflow:
        async with self.session_factory() as session:
            try:
                async with session.begin():
                    session.add(workflow)
            except IntegrityError as exc:  # unique(identifier) violated → already registered
                raise WorkflowAlreadyExistsError(workflow.identifier) from exc
            session.expunge(workflow)
            return workflow

    async def get_by_identifier(self, identifier: str) -> Workflow | None:
        async with self.session_factory() as session:
            wf = (
                (await session.execute(select(Workflow).where(Workflow.identifier == identifier)))
                .scalars()
                .first()
            )
            if wf is not None:
                session.expunge(wf)
            return wf

    async def update(self, workflow: Workflow) -> Workflow | None:
        async with self.session_factory() as session:
            async with session.begin():
                db = (
                    (
                        await session.execute(
                            select(Workflow).where(Workflow.identifier == workflow.identifier)
                        )
                    )
                    .scalars()
                    .first()
                )
                if db is None:
                    return None
                db.name = workflow.name
                db.run_type = workflow.run_type
                db.engine_type = workflow.engine_type
                db.automation_id = workflow.automation_id
                db.ticket_template_id = workflow.ticket_template_id
            session.expunge(db)
            return db

    async def list(self) -> list[Workflow]:
        async with self.session_factory() as session:
            rows = list((await session.execute(select(Workflow))).scalars().all())
            for wf in rows:
                session.expunge(wf)
            return rows
