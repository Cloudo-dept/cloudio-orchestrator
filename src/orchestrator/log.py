"""Logging configuration: stdlib ``logging``, ECS-formatted JSON on stdout.

One ``configure_logging`` call per process entrypoint — ``orchestrator worker``, ``orchestrator
api``, and the API lifespan (so a container that runs ``uvicorn`` directly is covered too).
Business logic never configures logging: it does ``logger = logging.getLogger(__name__)`` and logs
with ``%``-style lazy args; the handler installed here decides format and threshold.

The shape of logging is a ``logging.config.dictConfig`` document mounted at ``LOG_CONFIG_PATH``:
``ecs_logging.StdlibFormatter`` on a single ``StreamHandler`` bound to stdout, with no file
handler anywhere. When that file is absent (local ``uv run``) we fall back to a plain readable
one-line format on stdout — ECS JSON is what the log shipper wants, not what a developer reading a
terminal wants, and the mounted file is the thing that declares "this process is shipped".

Security: ``StdlibFormatter`` renders ``error.stack_trace`` with ``traceback.format_tb``, which
never includes local variables. That preserves the property the old loguru sink got from
``diagnose=False`` — locals here hold ServiceNow/Airflow credentials.
"""

import json
import logging
import logging.config
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar, Token
from pathlib import Path
from typing import Any, Final

from starlette.requests import Request
from starlette.types import ASGIApp, Receive, Scope, Send

from orchestrator.domain import StepName, WorkflowRun

# The runtime log configuration, mounted read-only by the deployment. Absolute on purpose: the
# image's WORKDIR is /app (the code) while /opt/app/config is the config mount, so resolution
# never depends on the process cwd and the two never collide.
LOG_CONFIG_PATH: Final[Path] = Path("/opt/app/config/log-config.json")

# Fallback format, used only when LOG_CONFIG_PATH is absent. Deliberately human-readable.
_FALLBACK_FORMAT: Final[str] = "%(asctime)s | %(levelname)-8s | %(name)s:%(lineno)d - %(message)s"

# Custom ECS fields carrying the in-flight request. The dots are load-bearing: StdlibFormatter
# de-dots extra record attributes, so these are emitted as nested objects under "cloudio" — a
# namespace we own, which is where ECS expects non-standard fields to live.
HTTP_URL_FIELD: Final[str] = "cloudio.operation.http.url"
HTTP_QUERY_PARAMS_FIELD: Final[str] = "cloudio.operation.http.query_params"
HTTP_PATH_PARAMS_FIELD: Final[str] = "cloudio.operation.http.path_params"

# Custom ECS fields carrying the run being driven. These are what make a run debuggable: no run
# identifier crosses a port boundary (ports.py keeps provider vocabulary and run vocabulary apart),
# so without a ContextVar the adapters — where failures actually happen — cannot name the run they
# are working for. Binding once in RunExecutor.handle covers executor, step handlers, adapters and
# escalator in one go, with no signature changes and nothing leaking through the ports.
RUN_ID_FIELD: Final[str] = "cloudio.run.id"
RUN_TYPE_FIELD: Final[str] = "cloudio.run.type"
RUN_STATUS_FIELD: Final[str] = "cloudio.run.status"
RUN_STEP_FIELD: Final[str] = "cloudio.run.step"
RUN_ATTEMPT_FIELD: Final[str] = "cloudio.run.attempt"
RUN_WORKFLOW_FIELD: Final[str] = "cloudio.run.workflow"
RUN_CREATED_BY_FIELD: Final[str] = "cloudio.run.created_by"
RUN_VERSION_FIELD: Final[str] = "cloudio.run.version"
RUN_TICKET_ID_FIELD: Final[str] = "cloudio.run.ticket_id"
RUN_ENGINE_RUN_ID_FIELD: Final[str] = "cloudio.run.engine_run_id"
WORKER_ID_FIELD: Final[str] = "cloudio.worker.id"

# Set by RequestContextMiddleware, read lazily by RequestContextFilter at emit time.
_request: ContextVar[Request | None] = ContextVar("cloudio_request", default=None)

# Set by run_log_context / worker_log_context, read lazily by RunContextFilter at emit time.
_run: ContextVar[WorkflowRun | None] = ContextVar("cloudio_run", default=None)
_worker_id: ContextVar[int | None] = ContextVar("cloudio_worker_id", default=None)

logger = logging.getLogger(__name__)


class RequestContextFilter(logging.Filter):
    """Injects the current request's URL, query params and path params onto every record.

    The three values are read *lazily*, here at emit time, from the live ``Request``. That is what
    makes ``path_params`` correct: Starlette only fills ``scope["path_params"]`` while routing,
    which happens *after* middleware runs, so a snapshot taken in the middleware would always be
    empty. The middleware stores the ``Request``, whose ``scope`` is the very dict the router
    mutates.

    Records with no request in scope (the worker daemon, lifespan, uvicorn startup) pass through
    untouched — absent is more honest than empty.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        request = _request.get()
        if request is not None:
            record.__dict__[HTTP_URL_FIELD] = str(request.url)
            record.__dict__[HTTP_QUERY_PARAMS_FIELD] = str(request.query_params)
            path_params = {str(key): str(value) for key, value in request.path_params.items()}
            if path_params:  # omit the field entirely rather than emit an empty object
                record.__dict__[HTTP_PATH_PARAMS_FIELD] = path_params
        return True  # a context filter enriches records, it never drops them


class RunContextFilter(logging.Filter):
    """Injects the run being driven, and the worker loop driving it, onto every record.

    Read *lazily* at emit time from the live ``WorkflowRun``, for the same reason ``path_params``
    is: ``RunExecutor.handle`` walks several steps in one call, reassigning ``run.current_step``
    each iteration, so a snapshot taken at bind time would pin the first step for the whole drive.
    Reading the live object tracks the loop for free.

    Every value is coerced explicitly. ``run_id`` is a ``uuid.UUID`` and the rest are ``str``-Enums
    whose ``__str__`` renders the *member* (``RunType.RESOURCE``), not the value — left raw,
    ecs-logging would emit ``"UUID('...')"`` and ``"RunType.RESOURCE"``. Scalars only: a mapping
    under a ``cloudio.*`` key whose own keys contain dots makes ecs-logging's ``merge_dicts`` raise,
    and ``logging`` drops the record in ``handleError``.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        worker_id = _worker_id.get()
        if worker_id is not None:
            record.__dict__[WORKER_ID_FIELD] = worker_id

        run = _run.get()
        if run is None:
            return True  # API requests, lifespan, idle worker polls — no run in scope
        record.__dict__[RUN_ID_FIELD] = str(run.run_id)
        record.__dict__[RUN_TYPE_FIELD] = run.run_type.value
        record.__dict__[RUN_STATUS_FIELD] = run.status.value
        record.__dict__[RUN_WORKFLOW_FIELD] = run.workflow_identifier
        record.__dict__[RUN_CREATED_BY_FIELD] = run.created_by
        record.__dict__[RUN_VERSION_FIELD] = run.version
        if run.current_step is not None:
            step = run.current_step
            record.__dict__[RUN_STEP_FIELD] = step.value if isinstance(step, StepName) else step
            record.__dict__[RUN_ATTEMPT_FIELD] = run.run_state.step_attempts.get(step, 0) + 1
        if run.run_state.ticket is not None:
            record.__dict__[RUN_TICKET_ID_FIELD] = run.run_state.ticket.ticket_id
        if run.run_state.engine_run_id is not None:
            record.__dict__[RUN_ENGINE_RUN_ID_FIELD] = run.run_state.engine_run_id
        return True


@contextmanager
def run_log_context(run: WorkflowRun) -> Iterator[None]:
    """Publish ``run`` for ``RunContextFilter`` for the duration of the block."""
    token = _run.set(run)
    try:
        yield
    finally:
        _run.reset(token)


@contextmanager
def worker_log_context(worker_id: int) -> Iterator[None]:
    """Publish the worker loop id for ``RunContextFilter`` for the duration of the block."""
    token = _worker_id.set(worker_id)
    try:
        yield
    finally:
        _worker_id.reset(token)


class RequestContextMiddleware:
    """Pure-ASGI middleware that publishes the current request for ``RequestContextFilter``.

    Pure ASGI rather than ``BaseHTTPMiddleware`` on purpose: it calls the downstream app in the
    *same* task, so the ContextVar is unambiguously visible to routing, the endpoint, and uvicorn's
    ``send`` (which is where the ``uvicorn.access`` record is emitted). ``BaseHTTPMiddleware`` runs
    the app in a child anyio task behind two memory object streams, and we need the raw scope
    rather than a buffered response, so its ``call_next`` abstraction buys us nothing here.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":  # lifespan / websocket: nothing request-scoped to publish
            await self.app(scope, receive, send)
            return
        token: Token[Request | None] = _request.set(Request(scope))
        try:
            await self.app(scope, receive, send)
        except Exception:
            # Log here, not in an exception handler, because *here* the context is still bound.
            # Starlette's ServerErrorMiddleware sits outside this middleware, so by the time it
            # logs the 500 through uvicorn.error our `finally` has already reset the ContextVar —
            # leaving the tracebacks that matter most as the only records with no request context.
            logger.exception("Unhandled exception serving %s %s.", scope["method"], scope["path"])
            raise
        finally:
            _request.reset(token)


def log_config_exists(config_path: Path = LOG_CONFIG_PATH) -> bool:
    """Whether the mounted log configuration is there.

    ``is_file()``, not ``exists()``: docker creates a *directory* at a bind-mount target whose host
    source is missing, and a directory has to count as absent (reading it would raise).
    """
    return config_path.is_file()


def _install_context_filters() -> None:
    """Attach the context filters to every root handler, whichever branch configured logging.

    Deliberately code, not config. Declaring the filters in the dictConfig document would mean the
    fallback branch (``basicConfig``, no filters) silently loses correlation, and a deployment that
    mounts its own document would lose it too — a document is free to omit a ``filters`` block, and
    the failure mode is invisible. Enrichment is application behaviour; the document owns format
    and destination. Idempotent by filter type, so a document that *does* declare them is fine.
    """
    for handler in logging.getLogger().handlers:
        installed = {type(existing) for existing in handler.filters}
        for filter_class in (RequestContextFilter, RunContextFilter):
            if filter_class not in installed:
                handler.addFilter(filter_class())


def configure_logging(level: str = "INFO", config_path: Path = LOG_CONFIG_PATH) -> None:
    """Install the process-wide logging configuration. Safe to call more than once.

    File present -> that dictConfig document (ECS JSON on stdout). File absent -> a plain
    one-line format on stdout. Either way there is exactly one sink and it is stdout.

    Idempotent: ``dictConfig`` replaces a logger's handler list rather than appending to it, and
    ``basicConfig(force=True)`` clears root first — so the api path (CLI -> uvicorn -> lifespan,
    three calls) never doubles a line.
    """
    if log_config_exists(config_path):
        config: dict[str, Any] = json.loads(config_path.read_text(encoding="utf-8"))
        logging.config.dictConfig(config)
        source = str(config_path)
    else:
        logging.basicConfig(
            stream=sys.stdout, format=_FALLBACK_FORMAT, level=logging.INFO, force=True
        )
        source = f"built-in default (no file at {config_path})"

    # The document owns the shape of logging; ORCH_LOG_LEVEL owns the threshold. Applied after, so
    # the environment always wins, and resolved by name so a typo degrades to INFO instead of
    # raising inside an entrypoint.
    levels = logging.getLevelNamesMapping()
    resolved = levels.get(level.upper(), logging.INFO)
    logging.getLogger().setLevel(resolved)
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logging.getLogger(name).setLevel(resolved)

    _install_context_filters()

    logger = logging.getLogger(__name__)
    if level.upper() not in levels:
        logger.warning("Unknown log level %r; falling back to INFO.", level)
    logger.info(
        "Logging configured from %s; stdout sink at level %s.",
        source,
        logging.getLevelName(resolved),
    )
