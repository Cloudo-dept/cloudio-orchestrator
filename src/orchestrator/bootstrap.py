"""The composition root — the one place adapters are bound to ports (IoC)."""

import logging

import httpx
from pydantic import BaseModel, ConfigDict

from orchestrator.adapters.airflow import AirflowWorkflowEngineClient
from orchestrator.adapters.database import (
    PostgresHealthCheck,
    PostgresWorkflowRepository,
    PostgresWorkflowRunRepository,
    make_session_factory,
)
from orchestrator.adapters.logging_transport import FailureLoggingTransport
from orchestrator.adapters.project_manager import ProjectManagerResourceClient
from orchestrator.adapters.servicenow import ServiceNowTicketClient
from orchestrator.config import Settings
from orchestrator.domain import WorkflowEngineType
from orchestrator.orchestration.escalator import FailureEscalator
from orchestrator.orchestration.executor import RunExecutor
from orchestrator.orchestration.plans import build_handlers
from orchestrator.ports import HealthCheck, WorkflowEngineClient
from orchestrator.services import RunCallbackService, WorkflowRunService, WorkflowService
from orchestrator.worker import OrchestratorWorker

logger = logging.getLogger(__name__)


class Container(BaseModel):
    """Everything wired; entrypoints pick what they need (API: services; worker: worker)."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    workflow_service: WorkflowService
    run_service: WorkflowRunService
    callback_service: RunCallbackService
    worker: OrchestratorWorker
    health_check: HealthCheck
    # The pooled provider clients. Held only so an entrypoint can release their connections on
    # shutdown; nothing reads them. Adapters own the calls, the container owns the sockets.
    http_clients: list[httpx.AsyncClient]

    async def aclose(self) -> None:
        """Drain every provider connection pool. Safe to call twice."""
        for client in self.http_clients:
            await client.aclose()


def _provider_client(
    base_url: str,
    settings: Settings,
    transport: httpx.AsyncBaseTransport,
    *,
    headers: dict[str, str] | None = None,
    auth: tuple[str, str] | None = None,
) -> httpx.AsyncClient:
    """One long-lived, pooled client for a provider.

    The connection limits go on the *transport*, not the client: an explicit ``transport=`` makes
    the client's own ``limits`` (like its ``verify``) inert, so setting them there would look right
    and bound nothing.
    """
    return httpx.AsyncClient(
        base_url=base_url.rstrip("/"),
        # connect/read/write get the provider budget; waiting for a pooled connection gets its own
        # (see http_pool_acquire_timeout_seconds). Timeouts, unlike limits, do apply client-side.
        timeout=httpx.Timeout(
            settings.external_call_timeout_seconds,
            pool=settings.http_pool_acquire_timeout_seconds,
        ),
        transport=transport,
        headers={"Accept": "application/json", **(headers or {})},
        auth=auth,
    )


async def build(settings: Settings) -> Container:
    logger.info("Composition root: wiring adapters to ports.")
    session_factory = make_session_factory(settings.database_url)
    runs = PostgresWorkflowRunRepository(session_factory)
    workflows = PostgresWorkflowRepository(session_factory)
    logger.debug("Postgres run store + workflow registry bound.")

    # One transport per provider, so a failed outbound call names the provider it was made to, and
    # so each provider gets its own connection pool — the limits live on the transport, and a
    # shared one would let a burst against Airflow starve ServiceNow of connections.
    # TLS verification is disabled on every provider transport: client-level TLS arguments are inert
    # once an explicit transport is supplied, so ``verify=False`` must go on the transport itself.
    limits = httpx.Limits(
        max_connections=settings.http_max_connections,
        max_keepalive_connections=settings.http_max_keepalive_connections,
    )
    servicenow_http = _provider_client(
        settings.servicenow_base_url,
        settings,
        FailureLoggingTransport(
            "ServiceNow",
            httpx.AsyncHTTPTransport(verify=False, limits=limits),  # noqa: S501 — private cloud
        ),
        auth=(settings.servicenow_username, settings.servicenow_password.get_secret_value()),
    )
    pm_http = _provider_client(
        settings.pm_base_url,
        settings,
        FailureLoggingTransport(
            "Project Manager",
            httpx.AsyncHTTPTransport(verify=False, limits=limits),  # noqa: S501 — private cloud
        ),
        headers={"Authorization": f"Bearer {settings.pm_token.get_secret_value()}"},
    )
    airflow_http = _provider_client(
        settings.airflow_base_url,
        settings,
        FailureLoggingTransport(
            "Airflow",
            httpx.AsyncHTTPTransport(verify=False, limits=limits),  # noqa: S501 — per spec
        ),
    )

    ticket_client = ServiceNowTicketClient(
        servicenow_http,
        settings.servicenow_responsible_groups,
        settings.servicenow_incident_team,
        group_lookup_field=settings.servicenow_group_lookup_field,
        user_lookup_field=settings.servicenow_user_lookup_field,
    )
    resource_client = ProjectManagerResourceClient(pm_http)
    engines: dict[WorkflowEngineType, WorkflowEngineClient] = {
        WorkflowEngineType.AIRFLOW: AirflowWorkflowEngineClient(
            airflow_http,
            settings.airflow_username,
            settings.airflow_password.get_secret_value(),
        ),
    }

    logger.debug(
        "Adapters bound: ServiceNow ticket system, Project Manager resource client, engines=%s.",
        [e.value for e in engines],
    )

    handlers = build_handlers(ticket_client, resource_client, engines)
    escalator = FailureEscalator(ticket_client, resource_client, settings.servicenow_incident_team)
    executor = RunExecutor(handlers, runs, settings, escalator)  # sets scheduled_at
    worker = OrchestratorWorker(
        runs,
        executor,
        concurrency_limit=settings.worker_concurrency_limit,
        poll_interval_seconds=settings.worker_poll_interval_seconds,
        lease_seconds=settings.redrive_lease_seconds,
    )
    logger.info(
        "Container built: %s step handlers, worker concurrency=%s, re-drive lease=%ss, "
        "HTTP pool per provider=%s (keepalive %s).",
        len(handlers),
        settings.worker_concurrency_limit,
        settings.redrive_lease_seconds,
        settings.http_max_connections,
        settings.http_max_keepalive_connections,
    )

    return Container(
        workflow_service=WorkflowService(workflows),
        run_service=WorkflowRunService(runs, workflows),
        callback_service=RunCallbackService(runs),
        worker=worker,
        health_check=PostgresHealthCheck(session_factory),
        http_clients=[servicenow_http, pm_http, airflow_http],
    )
