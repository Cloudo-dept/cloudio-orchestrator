"""Dev-only Project Manager mock.

A FastAPI + Postgres stand-in for the real Project Manager API. It implements the resource
endpoints the orchestrator's ``ProjectManagerResourceClient`` calls, plus convenience endpoints
(list, and project creation) for seeding and eyeballing state during development. State persists
in a dedicated Postgres database so it survives restarts.

NOT for production — no auth is enforced and the schema is created on startup (no migrations).
"""

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Header, HTTPException
from sqlalchemy import String, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

DATABASE_URL = os.environ.get("PM_DATABASE_URL", "postgresql+asyncpg://pm:pm@pm-postgres:5432/pm")


class Base(DeclarativeBase):
    pass


class Project(Base):
    __tablename__ = "projects"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    project_id: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(255))
    body: Mapped[dict[str, Any]] = mapped_column(JSONB)


class Resource(Base):
    __tablename__ = "resources"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    idempotency_key: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    project_id: Mapped[str] = mapped_column(String(255), index=True)
    resource_type: Mapped[str] = mapped_column(String(255))
    vendor_id: Mapped[str] = mapped_column(String(255), index=True)
    body: Mapped[dict[str, Any]] = mapped_column(JSONB)


engine = create_async_engine(DATABASE_URL)
Session = async_sessionmaker(engine, expire_on_commit=False)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)  # dev convenience: no Alembic
    yield


app = FastAPI(title="Project Manager (dev mock)", lifespan=lifespan)


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/projects")
async def create_project(body: dict[str, Any]) -> dict[str, Any]:
    """Create a project. Idempotent on the caller-supplied ``project_id`` (a repeat returns the
    existing project rather than erroring)."""
    project_id = body.get("project_id")
    if not project_id:
        raise HTTPException(422, detail="project_id is required")
    async with Session() as session:
        existing = (
            await session.execute(select(Project).where(Project.project_id == project_id))
        ).scalar_one_or_none()
        if existing is not None:
            return existing.body

        project = {**body, "name": body.get("name", project_id)}
        session.add(Project(project_id=project_id, name=project["name"], body=project))
        await session.commit()
        return project


@app.get("/projects")
async def list_projects() -> list[dict[str, Any]]:
    """Convenience for eyeballing state during development."""
    async with Session() as session:
        rows = (await session.execute(select(Project))).scalars().all()
        return [r.body for r in rows]


@app.post("/projects/{project_id}/project_resources/{resource_type}")
async def create_resource(
    project_id: str,
    resource_type: str,
    body: dict[str, Any],
    idempotency_key: str = Header(alias="Idempotency-Key"),
) -> dict[str, Any]:
    async with Session() as session:
        # Idempotent on the orchestrator-supplied key, and on the natural key (vendor_id).
        by_key = (
            await session.execute(
                select(Resource).where(Resource.idempotency_key == idempotency_key)
            )
        ).scalar_one_or_none()
        if by_key is not None:
            return by_key.body

        vendor_id = body["vendor_id"]
        by_vendor = (
            (
                await session.execute(
                    select(Resource).where(
                        Resource.project_id == project_id,
                        Resource.resource_type == resource_type,
                        Resource.vendor_id == vendor_id,
                    )
                )
            )
            .scalars()
            .first()
        )
        if by_vendor is not None:
            return by_vendor.body

        resource = {**body, "in_progress": body.get("in_progress", True)}
        session.add(
            Resource(
                idempotency_key=idempotency_key,
                project_id=project_id,
                resource_type=resource_type,
                vendor_id=vendor_id,
                body=resource,
            )
        )
        await session.commit()
        return resource


@app.patch("/projects/{project_id}/project_resources/{resource_type}/{vendor_id}")
async def update_resource(
    project_id: str,
    resource_type: str,
    vendor_id: str,
    body: dict[str, Any],
) -> dict[str, Any]:
    async with Session() as session:
        row = (
            (
                await session.execute(
                    select(Resource).where(
                        Resource.project_id == project_id,
                        Resource.resource_type == resource_type,
                        Resource.vendor_id == vendor_id,
                    )
                )
            )
            .scalars()
            .first()
        )
        if row is None:
            raise HTTPException(404, detail=f"resource '{vendor_id}' not found")
        row.body = {**row.body, **body}  # whole-value reassignment → tracked by SQLAlchemy
        # A finalize may carry the real vendor id the engine provisioned; re-key the record to it
        # so it is findable under its true identity (the run-id was only a create-time placeholder).
        new_vendor_id = body.get("vendor_id")
        if new_vendor_id:
            row.vendor_id = new_vendor_id
        await session.commit()
        return row.body


@app.get("/projects/{project_id}/project_resources/{resource_type}")
async def list_resources(project_id: str, resource_type: str) -> list[dict[str, Any]]:
    """Convenience for eyeballing state during development."""
    async with Session() as session:
        rows = (
            (
                await session.execute(
                    select(Resource).where(
                        Resource.project_id == project_id,
                        Resource.resource_type == resource_type,
                    )
                )
            )
            .scalars()
            .all()
        )
        return [r.body for r in rows]
