"""Postgres repository behaviour: JSONB round-trip, optimistic concurrency, claim_due, finders.

Requires Docker (testcontainers Postgres); skipped otherwise via the postgres_url fixture.
"""

import asyncio

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker

from orchestrator.adapters.database import (
    PostgresWorkflowRepository,
    PostgresWorkflowRunRepository,
)
from orchestrator.domain import RunStatus, RunType, StaleRunError, TicketRef, utcnow
from tests.factories import make_run, make_workflow

pytestmark = pytest.mark.integration


async def test_run_roundtrips_through_jsonb(pg_session_factory: async_sessionmaker) -> None:
    repo = PostgresWorkflowRunRepository(pg_session_factory)
    run = await repo.create(make_run(run_type=RunType.RESOURCE))
    got = await repo.get(run.run_id)
    assert got is not None
    assert got.run_type is RunType.RESOURCE
    assert got.status is RunStatus.PENDING
    assert got.run_state == run.run_state  # whole typed state survives the trip
    assert got.run_state.resource is not None
    assert got.run_state.resource.resource_type == "vm"


async def test_optimistic_concurrency_conflict(pg_session_factory: async_sessionmaker) -> None:
    repo = PostgresWorkflowRunRepository(pg_session_factory)
    run = await repo.create(make_run())
    a = await repo.get(run.run_id)
    b = await repo.get(run.run_id)
    assert a is not None and b is not None

    a.status = RunStatus.RUNNING
    await repo.save(a)  # v1 -> v2
    b.status = RunStatus.FAILED
    with pytest.raises(StaleRunError):
        await repo.save(b)  # still on v1 -> conflict


async def test_claim_due_selects_due_and_leases_forward(
    pg_session_factory: async_sessionmaker,
) -> None:
    repo = PostgresWorkflowRunRepository(pg_session_factory)
    due = await repo.create(make_run())  # scheduled_at defaults to now
    future = await repo.get((await repo.create(make_run())).run_id)
    assert future is not None
    future.scheduled_at = utcnow().replace(year=utcnow().year + 1)
    await repo.save(future)

    claimed = await repo.claim_due(10, lease_seconds=300)
    assert due.run_id in claimed and future.run_id not in claimed
    # The just-claimed run was leased forward, so an immediate re-claim returns nothing.
    assert await repo.claim_due(10, lease_seconds=300) == []


async def test_claim_due_is_disjoint_per_call(pg_session_factory: async_sessionmaker) -> None:
    repo = PostgresWorkflowRunRepository(pg_session_factory)
    ids = {(await repo.create(make_run())).run_id for _ in range(3)}
    got = set()
    for _ in range(3):
        claimed = await repo.claim_due(1, lease_seconds=300)
        assert len(claimed) == 1
        got.add(claimed[0])
    assert got == ids  # each returned exactly once
    assert await repo.claim_due(1, lease_seconds=300) == []


async def test_lease_expiry_re_exposes_the_run(pg_session_factory: async_sessionmaker) -> None:
    repo = PostgresWorkflowRunRepository(pg_session_factory)
    run = await repo.create(make_run())
    assert await repo.claim_due(1, lease_seconds=0.5) == [run.run_id]
    assert await repo.claim_due(1, lease_seconds=0.5) == []  # leased, not due yet
    await asyncio.sleep(0.7)
    assert await repo.claim_due(1, lease_seconds=0.5) == [run.run_id]  # lease expired → re-drive


async def test_find_by_ticket_and_resource(pg_session_factory: async_sessionmaker) -> None:
    repo = PostgresWorkflowRunRepository(pg_session_factory)
    run = await repo.create(make_run(run_type=RunType.RESOURCE))
    loaded = await repo.get(run.run_id)
    assert loaded is not None
    loaded.run_state.ticket = TicketRef(ticket_id="RITM0009999", native_id="sys9")
    await repo.save(loaded)

    by_ticket = await repo.find_by_ticket_id("RITM0009999")
    assert [r.run_id for r in by_ticket] == [run.run_id]
    by_resource = await repo.find_by_resource_id("vm-1")
    assert [r.run_id for r in by_resource] == [run.run_id]
    assert await repo.find_by_ticket_id("absent") == []


async def test_find_by_engine_run_id(pg_session_factory: async_sessionmaker) -> None:
    repo = PostgresWorkflowRunRepository(pg_session_factory)
    run = await repo.create(make_run())
    loaded = await repo.get(run.run_id)
    assert loaded is not None
    loaded.run_state.engine_run_id = "dagrun-42"
    await repo.save(loaded)

    found = await repo.find_by_engine_run_id("dagrun-42")
    assert [r.run_id for r in found] == [run.run_id]
    assert await repo.find_by_engine_run_id("absent") == []


async def test_wake_makes_a_waiting_run_due_now(pg_session_factory: async_sessionmaker) -> None:
    repo = PostgresWorkflowRunRepository(pg_session_factory)
    run = await repo.create(make_run())
    loaded = await repo.get(run.run_id)
    assert loaded is not None
    loaded.status = RunStatus.RUNNING
    loaded.scheduled_at = utcnow().replace(year=utcnow().year + 1)  # far-future poll
    await repo.save(loaded)
    assert await repo.claim_due(1, lease_seconds=300) == []  # not due yet

    assert await repo.wake(run.run_id) is True
    woken = await repo.get(run.run_id)
    assert woken is not None and woken.scheduled_at is not None
    assert woken.scheduled_at <= utcnow()  # now due
    assert await repo.claim_due(1, lease_seconds=300) == [run.run_id]  # a worker picks it up now


async def test_wake_is_a_noop_for_a_terminal_run(pg_session_factory: async_sessionmaker) -> None:
    repo = PostgresWorkflowRunRepository(pg_session_factory)
    run = await repo.create(make_run())
    loaded = await repo.get(run.run_id)
    assert loaded is not None
    loaded.status = RunStatus.COMPLETED
    loaded.scheduled_at = None  # terminal runs carry no schedule
    await repo.save(loaded)

    assert await repo.wake(run.run_id) is False
    still = await repo.get(run.run_id)
    assert still is not None and still.scheduled_at is None  # untouched


async def test_wake_does_not_bump_version(pg_session_factory: async_sessionmaker) -> None:
    # wake is a nudge outside the optimistic-version scheme, so a run loaded before the wake can
    # still be saved afterwards without a StaleRunError.
    repo = PostgresWorkflowRunRepository(pg_session_factory)
    run = await repo.create(make_run())
    held = await repo.get(run.run_id)
    assert held is not None

    assert await repo.wake(run.run_id) is True
    held.status = RunStatus.RUNNING
    await repo.save(held)  # must not raise — wake left version alone


async def test_workflow_registry_roundtrip(pg_session_factory: async_sessionmaker) -> None:
    repo = PostgresWorkflowRepository(pg_session_factory)
    await repo.register(make_workflow(identifier="provision-vm", run_type=RunType.RESOURCE))
    got = await repo.get_by_identifier("provision-vm")
    assert got is not None and got.automation_id == "dag-x"
    assert [w.identifier for w in await repo.list()] == ["provision-vm"]
    assert await repo.get_by_identifier("ghost") is None
