"""Logging configuration tests.

Deliberately not using pytest's ``caplog``: it attaches its own handler to root, and every
``dictConfig`` call clears root's handlers, so ``caplog`` would silently capture nothing. These
tests drive the real handler and read the bytes it writes.
"""

import io
import json
import logging
import logging.config
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import ecs_logging
import pytest
from starlette.requests import Request

from orchestrator.log import (
    LOG_CONFIG_PATH,
    RequestContextFilter,
    RunContextFilter,
    configure_logging,
    log_config_exists,
)

EXAMPLE_CONFIG = Path(__file__).parents[2] / "config" / "log-config.example.json"


@pytest.fixture(autouse=True)
def restore_logging() -> Iterator[None]:
    """Snapshot root's configuration; these tests reconfigure logging globally."""
    root = logging.getLogger()
    handlers, level = root.handlers[:], root.level
    yield
    root.handlers[:] = handlers
    root.setLevel(level)


def make_request(
    path: str = "/api/v1/workflows/provision-vm", query: str = "", **scope: Any
) -> Request:
    """A Starlette Request over a hand-built ASGI scope (no server needed)."""
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": path,
            "raw_path": path.encode(),
            "query_string": query.encode(),
            "headers": [],
            "scheme": "http",
            "server": ("testserver", 80),
            **scope,
        }
    )


def emit(request: Request | None, **formatter_kwargs: Any) -> dict[str, Any]:
    """Log one record through the real filter + ECS formatter; return the parsed JSON."""
    from orchestrator import log as log_module

    buffer = io.StringIO()
    handler = logging.StreamHandler(buffer)
    handler.setFormatter(ecs_logging.StdlibFormatter(**formatter_kwargs))
    handler.addFilter(RequestContextFilter())
    logger = logging.getLogger("test.emit")
    logger.handlers = [handler]
    logger.setLevel(logging.INFO)
    logger.propagate = False

    token = log_module._request.set(request)
    try:
        logger.info("Run %s executing step %s.", "run-1", "OPEN_TICKET")
    finally:
        log_module._request.reset(token)
    return json.loads(buffer.getvalue())


# --- the shipped document ---


def test_example_config_is_valid_dictconfig() -> None:
    config = json.loads(EXAMPLE_CONFIG.read_text())
    logging.config.dictConfig(config)

    root = logging.getLogger()
    assert len(root.handlers) == 1
    handler = root.handlers[0]
    assert isinstance(handler, logging.StreamHandler)
    assert handler.stream is sys.stdout
    assert isinstance(handler.formatter, ecs_logging.StdlibFormatter)


def test_context_filters_are_installed_by_code_not_config(tmp_path: Path) -> None:
    """Enrichment is application behaviour, so it must survive a document that does not declare it
    — including the fallback branch, which installs no filters at all."""
    config_path = tmp_path / "log-config.json"
    config_path.write_text(EXAMPLE_CONFIG.read_text())

    for path in (config_path, tmp_path / "absent.json"):  # both branches
        configure_logging("INFO", path)
        filters = [type(f) for f in logging.getLogger().handlers[0].filters]
        assert RequestContextFilter in filters, path
        assert RunContextFilter in filters, path


def test_installing_filters_is_idempotent(tmp_path: Path) -> None:
    for _ in range(3):
        configure_logging("INFO", tmp_path / "absent.json")

    filters = [type(f) for f in logging.getLogger().handlers[0].filters]
    assert filters.count(RequestContextFilter) == 1
    assert filters.count(RunContextFilter) == 1


def test_no_file_handlers_anywhere() -> None:
    """Requirement: all output via stdout, no custom log files."""
    logging.config.dictConfig(json.loads(EXAMPLE_CONFIG.read_text()))

    loggers = [logging.getLogger()] + [
        obj for obj in logging.Logger.manager.loggerDict.values() if isinstance(obj, logging.Logger)
    ]
    for logger in loggers:
        for handler in logger.handlers:
            assert not isinstance(handler, logging.FileHandler), f"{logger.name} writes to a file"


def test_uvicorn_loggers_propagate_to_the_ecs_handler() -> None:
    """uvicorn's own config gives these dedicated handlers and propagate=False; ours must undo
    that, or uvicorn's startup and access lines never reach the ECS stdout handler."""
    logging.config.dictConfig(json.loads(EXAMPLE_CONFIG.read_text()))

    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logger = logging.getLogger(name)
        assert logger.handlers == [], f"{name} kept its own handler"
        assert logger.propagate is True, f"{name} would not reach root"
        assert logger.hasHandlers(), f"{name} has no reachable handler"


# --- the custom HTTP fields ---


def test_custom_http_fields_are_nested_under_cloudio() -> None:
    request = make_request(query="ticket_id=RITM0012", path_params={"identifier": "provision-vm"})
    http = emit(request)["cloudio"]["operation"]["http"]

    assert http["url"] == "http://testserver/api/v1/workflows/provision-vm?ticket_id=RITM0012"
    assert http["query_params"] == "ticket_id=RITM0012"
    assert http["path_params"] == {"identifier": "provision-vm"}


def test_path_params_are_read_lazily_after_routing() -> None:
    """The ordering guarantee the whole design rests on.

    Starlette fills scope["path_params"] while *routing*, which happens after the middleware has
    already bound the request. A snapshot taken at bind time would be empty forever; reading the
    live scope at emit time sees the routed values. This test fails on any snapshot implementation.
    """
    request = make_request()  # no path_params yet — exactly the state at middleware entry
    assert request.path_params == {}

    request.scope["path_params"] = {"run_id": "b0a1f4de-0000-0000-0000-000000000000"}  # router runs

    http = emit(request)["cloudio"]["operation"]["http"]
    assert http["path_params"] == {"run_id": "b0a1f4de-0000-0000-0000-000000000000"}


def test_non_serializable_path_param_values_are_coerced() -> None:
    """run_id is declared as uuid.UUID, so the router puts a UUID object in the scope. Left raw,
    ecs-logging falls back to repr() and emits "UUID('...')"."""
    import uuid

    run_id = uuid.UUID("b0a1f4de-0000-0000-0000-000000000000")
    request = make_request(path_params={"run_id": run_id})

    assert emit(request)["cloudio"]["operation"]["http"]["path_params"] == {"run_id": str(run_id)}


def test_dotted_query_key_does_not_destroy_the_record() -> None:
    """Regression test for a remotely-triggerable log-suppression bug.

    ecs-logging de-dots extra keys, so a *mapping* value whose keys contain dots makes merge_dicts
    raise TypeError ("?a=1&a.b=2" collides `a` with `{b: ...}`). logging swallows that in
    handleError, so the record is lost and "--- Logging error ---" goes to stderr. Emitting
    query_params as a urlencoded string — which is exactly str(request.query_params) — is what
    makes an arbitrary query string safe.
    """
    request = make_request(query="a=1&a.b=2&ticket_id=RITM0012")

    http = emit(request)["cloudio"]["operation"]["http"]
    assert http["query_params"] == "a=1&a.b=2&ticket_id=RITM0012"
    assert isinstance(http["query_params"], str)


def test_records_without_a_request_are_untouched() -> None:
    """The worker daemon and lifespan log outside any request; absent beats empty."""
    payload = emit(None)
    assert "cloudio" not in payload


def test_empty_path_params_field_is_omitted() -> None:
    payload = emit(make_request(path_params={}))
    assert "path_params" not in payload["cloudio"]["operation"]["http"]


# --- ECS shape and the traceback security property ---


def test_emits_parseable_ecs_json() -> None:
    payload = emit(None, exclude_fields=["log.original"])

    assert payload["log.level"] == "info"
    assert payload["message"] == "Run run-1 executing step OPEN_TICKET."
    assert payload["@timestamp"].endswith("Z")
    assert payload["ecs"]["version"]
    assert "original" not in payload["log"]  # exclude_fields reached the formatter


def test_stack_trace_never_renders_local_variables() -> None:
    """The property the old loguru sink got from diagnose=False. Locals here hold ServiceNow and
    Airflow credentials, so a traceback that renders them would leak secrets into the log stream.
    """
    buffer = io.StringIO()
    handler = logging.StreamHandler(buffer)
    handler.setFormatter(ecs_logging.StdlibFormatter(stack_trace_limit=25))
    logger = logging.getLogger("test.trace")
    logger.handlers = [handler]
    logger.setLevel(logging.INFO)
    logger.propagate = False

    def escalate() -> None:
        servicenow_password = "SUPERSECRET-hunter2"  # noqa: F841 — the point of the test
        raise ValueError("kaboom")

    try:
        escalate()
    except ValueError:
        logger.exception("Failed to escalate run %s failure.", "run-1")

    raw = buffer.getvalue()
    payload = json.loads(raw)
    assert payload["error"]["type"] == "ValueError"
    assert payload["error"]["message"] == "kaboom"
    assert payload["error"]["stack_trace"]
    assert "SUPERSECRET" not in raw


# --- the file-present / file-absent branch ---


def test_configure_logging_uses_the_file_when_present(tmp_path: Path) -> None:
    config_path = tmp_path / "log-config.json"
    config_path.write_text(EXAMPLE_CONFIG.read_text())

    configure_logging("INFO", config_path)

    handler = logging.getLogger().handlers[0]
    assert isinstance(handler.formatter, ecs_logging.StdlibFormatter)


def test_configure_logging_falls_back_when_absent(tmp_path: Path) -> None:
    configure_logging("INFO", tmp_path / "nope.json")

    handlers = logging.getLogger().handlers
    assert len(handlers) == 1
    assert isinstance(handlers[0], logging.StreamHandler)
    assert handlers[0].stream is sys.stdout  # stdout even on the fallback path
    assert not isinstance(handlers[0].formatter, ecs_logging.StdlibFormatter)


def test_env_level_wins_over_the_document(tmp_path: Path) -> None:
    """The document says INFO; ORCH_LOG_LEVEL is applied after it and must win."""
    config_path = tmp_path / "log-config.json"
    config_path.write_text(EXAMPLE_CONFIG.read_text())

    configure_logging("DEBUG", config_path)

    assert logging.getLogger().level == logging.DEBUG
    assert logging.getLogger("uvicorn.access").level == logging.DEBUG


def test_unknown_level_degrades_to_info_instead_of_raising(tmp_path: Path) -> None:
    """TRACE was a loguru level and is advertised in older .env files; it must not kill startup."""
    configure_logging("TRACE", tmp_path / "nope.json")

    assert logging.getLogger().level == logging.INFO


def test_configure_logging_is_idempotent(tmp_path: Path) -> None:
    """The api path calls it three times (CLI -> uvicorn -> lifespan); lines must not multiply."""
    config_path = tmp_path / "log-config.json"
    config_path.write_text(EXAMPLE_CONFIG.read_text())

    for _ in range(3):
        configure_logging("INFO", config_path)

    assert len(logging.getLogger().handlers) == 1


def test_log_config_exists_rejects_a_directory(tmp_path: Path) -> None:
    """Docker creates a *directory* at a bind-mount target whose host source is missing, so
    exists() would report a broken mount as present and reading it would raise."""
    (tmp_path / "log-config.json").mkdir()

    assert log_config_exists(tmp_path / "log-config.json") is False
    assert log_config_exists(tmp_path / "absent.json") is False


def test_log_config_path_is_the_documented_mount_point() -> None:
    assert LOG_CONFIG_PATH == Path("/opt/app/config/log-config.json")
    assert LOG_CONFIG_PATH.is_absolute()  # independent of the image's WORKDIR
