"""Request log context through the real ASGI middleware stack.

The unit tests drive RequestContextFilter with a hand-built scope; these drive it through
RequestContextMiddleware and Starlette's router, which is the only way to prove that path_params
are populated by the time application code logs.

No production endpoint on a *templated* route logs today (the routes that log — the callbacks —
are untemplated), so the path_params cases run against a purpose-built probe app wired with the
same middleware. The production app is covered two ways: an assertion that it registers the
middleware, and a real callback request that exercises url/query_params in situ.
"""

import io
import json
import logging
import uuid
from collections.abc import AsyncIterator, Iterator
from types import SimpleNamespace
from typing import Any

import ecs_logging
import httpx
import pytest
from fastapi import FastAPI

from orchestrator.api import app
from orchestrator.log import RequestContextFilter, RequestContextMiddleware
from orchestrator.services import RunCallbackService, WorkflowRunService, WorkflowService
from tests.fakes import FakeHealthCheck, FakeWorkflowRepository, FakeWorkflowRunRepository

probe_logger = logging.getLogger("orchestrator.tests.probe")


@pytest.fixture
def ecs_stream() -> Iterator[io.StringIO]:
    """Attach the real ECS handler + filter to root for the duration of one test."""
    buffer = io.StringIO()
    handler = logging.StreamHandler(buffer)
    handler.setFormatter(ecs_logging.StdlibFormatter(exclude_fields=["log.original"]))
    handler.addFilter(RequestContextFilter())

    root = logging.getLogger()
    saved_handlers, saved_level = root.handlers[:], root.level
    root.handlers = [handler]
    root.setLevel(logging.INFO)
    # httpx logs one INFO line per call from the *client* side, after the response — outside the
    # request context, and pure noise here. The shipped config pins it to WARNING for the same
    # reason; mirror that so the assertions read against application records only.
    httpx_logger = logging.getLogger("httpx")
    saved_httpx = httpx_logger.level
    httpx_logger.setLevel(logging.WARNING)

    yield buffer

    root.handlers[:] = saved_handlers
    root.setLevel(saved_level)
    httpx_logger.setLevel(saved_httpx)


@pytest.fixture
async def probe_client() -> AsyncIterator[httpx.AsyncClient]:
    """A minimal app behind the real middleware, with templated routes that log."""
    probe = FastAPI()
    probe.add_middleware(RequestContextMiddleware)

    @probe.get("/probe/{identifier}")
    async def by_identifier(identifier: str) -> dict[str, str]:
        probe_logger.info("Handling probe for %s.", identifier)
        return {"identifier": identifier}

    @probe.get("/probe/run/{run_id}")
    async def by_run_id(run_id: uuid.UUID) -> dict[str, str]:
        probe_logger.info("Handling probe for run %s.", run_id)
        return {"run_id": str(run_id)}

    transport = httpx.ASGITransport(app=probe)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest.fixture
async def client(
    runs: FakeWorkflowRunRepository, workflows: FakeWorkflowRepository
) -> AsyncIterator[httpx.AsyncClient]:
    app.state.container = SimpleNamespace(
        workflow_service=WorkflowService(workflows),
        run_service=WorkflowRunService(runs, workflows),
        callback_service=RunCallbackService(runs),
        health_check=FakeHealthCheck(healthy=True),
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


def records(stream: io.StringIO) -> list[dict[str, Any]]:
    return [json.loads(line) for line in stream.getvalue().splitlines() if line.strip()]


def http_context(stream: io.StringIO) -> dict[str, Any]:
    """The HTTP context from the first record that carried one."""
    enriched = [r for r in records(stream) if "cloudio" in r]
    assert enriched, f"no record carried request context; got {records(stream)}"
    return enriched[0]["cloudio"]["operation"]["http"]


# --- path params: the lazy-read guarantee, end to end ---


async def test_path_params_reach_the_log_record(
    probe_client: httpx.AsyncClient, ecs_stream: io.StringIO
) -> None:
    """The end-to-end proof of the lazy read: Starlette fills scope["path_params"] while routing,
    which happens *after* the middleware bound the request, yet the endpoint's own log line still
    sees them. A snapshot taken at bind time would emit nothing here."""
    resp = await probe_client.get("/probe/provision-vm?ticket_id=RITM0012")
    assert resp.status_code == 200

    http = http_context(ecs_stream)
    assert http["path_params"] == {"identifier": "provision-vm"}
    assert http["query_params"] == "ticket_id=RITM0012"
    assert http["url"] == "http://test/probe/provision-vm?ticket_id=RITM0012"


async def test_uuid_path_param_is_serialized_as_a_string(
    probe_client: httpx.AsyncClient, ecs_stream: io.StringIO
) -> None:
    """A route declared `uuid.UUID` puts a UUID *object* in the scope; left raw, ecs-logging
    falls back to repr() and the field reads "UUID('...')"."""
    run_id = uuid.uuid4()
    resp = await probe_client.get(f"/probe/run/{run_id}")
    assert resp.status_code == 200

    assert http_context(ecs_stream)["path_params"] == {"run_id": str(run_id)}


async def test_dotted_query_key_still_yields_a_record(
    probe_client: httpx.AsyncClient, ecs_stream: io.StringIO
) -> None:
    """A caller-controlled dotted query key must not suppress the request's logs. Emitted as a
    mapping this raises TypeError inside ecs-logging's merge_dicts, which logging swallows in
    handleError — losing the record and printing "--- Logging error ---" to stderr."""
    resp = await probe_client.get("/probe/provision-vm?ticket_id=RITM0012&a.b=2")
    assert resp.status_code == 200

    assert "--- Logging error ---" not in ecs_stream.getvalue()
    assert http_context(ecs_stream)["query_params"] == "ticket_id=RITM0012&a.b=2"


# --- the production app ---


def test_production_app_registers_the_middleware() -> None:
    assert any(m.cls is RequestContextMiddleware for m in app.user_middleware)


async def test_real_endpoint_carries_the_http_context(
    client: httpx.AsyncClient, ecs_stream: io.StringIO
) -> None:
    """In situ on the production app: the ticket-approval callback logs from the service layer,
    and that record must carry the request's URL and query params."""
    resp = await client.post(
        "/api/v1/callbacks/ticket-approval?source=servicenow",
        json={"ticket_id": "RITM0012"},
    )
    assert resp.status_code == 202

    http = http_context(ecs_stream)
    assert http["url"] == "http://test/api/v1/callbacks/ticket-approval?source=servicenow"
    assert http["query_params"] == "source=servicenow"
    assert "path_params" not in http  # untemplated route — the field is omitted, not empty


async def test_context_does_not_leak_between_requests(
    client: httpx.AsyncClient, ecs_stream: io.StringIO
) -> None:
    """The middleware resets the ContextVar in a finally, so a later out-of-request log line must
    not inherit the previous request's URL. This matters because httpx.ASGITransport calls the app
    in the caller's context — an unreset var would bleed across tests and across worker tasks."""
    await client.post("/api/v1/callbacks/ticket-approval", json={"ticket_id": "RITM0012"})

    ecs_stream.truncate(0)
    ecs_stream.seek(0)
    logging.getLogger("orchestrator.worker").info("Worker loop #%d started.", 1)

    assert all("cloudio" not in record for record in records(ecs_stream))
