"""Alembic environment — async engine over the single ORCH_DATABASE_URL DSN."""

import asyncio
import os

from alembic import context
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import create_async_engine
from sqlmodel import SQLModel

import orchestrator.domain  # noqa: F401  (import populates SQLModel.metadata with the tables)

target_metadata = SQLModel.metadata


def _database_url() -> str:
    url = os.environ.get("ORCH_DATABASE_URL")
    if not url:
        raise RuntimeError("ORCH_DATABASE_URL must be set to run migrations.")
    return url


def run_migrations_offline() -> None:
    context.configure(url=_database_url(), target_metadata=target_metadata, literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()


def _do_run_migrations(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    engine = create_async_engine(_database_url())
    async with engine.connect() as connection:
        await connection.run_sync(_do_run_migrations)
    await engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())
