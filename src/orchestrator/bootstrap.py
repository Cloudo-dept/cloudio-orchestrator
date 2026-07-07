"""The composition root — the one place adapters are bound to ports (IoC)."""

from loguru import logger
from pydantic import BaseModel, ConfigDict

from orchestrator.adapters.airflow import AirflowWorkflowEngineClient
from orchestrator.adapters.database import (
    PostgresHealthCheck,
    PostgresWorkflowRepository,
    PostgresWorkflowRunRepository,
    make_session_factory,
)
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


class Container(BaseModel):
    """Everything wired; entrypoints pick what they need (API: services; worker: worker)."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    workflow_service: WorkflowService
    run_service: WorkflowRunService
    callback_service: RunCallbackService
    worker: OrchestratorWorker
    health_check: HealthCheck


async def build(settings: Settings) -> Container:
    logger.info("Composition root: wiring adapters to ports.")
    session_factory = make_session_factory(settings.database_url)
    runs = PostgresWorkflowRunRepository(session_factory)
    workflows = PostgresWorkflowRepository(session_factory)
    logger.debug("Postgres run store + workflow registry bound.")

    ticket_client = ServiceNowTicketClient(
        settings.servicenow_base_url,
        settings.servicenow_username,
        settings.servicenow_password.get_secret_value(),
        settings.servicenow_responsible_groups,
        settings.external_call_timeout_seconds,
    )
    resource_client = ProjectManagerResourceClient(
        settings.pm_base_url,
        settings.pm_token.get_secret_value(),
        settings.external_call_timeout_seconds,
    )
    engines: dict[WorkflowEngineType, WorkflowEngineClient] = {
        WorkflowEngineType.AIRFLOW: AirflowWorkflowEngineClient(
            settings.airflow_base_url,
            settings.airflow_username,
            settings.airflow_password.get_secret_value(),
            settings.external_call_timeout_seconds,
        ),
    }

    logger.debug(
        "Adapters bound: ServiceNow ticket system, Project Manager resource client, engines={}.",
        [e.value for e in engines],
    )

    handlers = build_handlers(ticket_client, resource_client, engines)
    escalator = FailureEscalator(ticket_client, settings.servicenow_incident_team)
    executor = RunExecutor(handlers, runs, settings, escalator)  # sets scheduled_at
    worker = OrchestratorWorker(
        runs,
        executor,
        concurrency_limit=settings.worker_concurrency_limit,
        poll_interval_seconds=settings.worker_poll_interval_seconds,
        lease_seconds=settings.redrive_lease_seconds,
    )
    logger.info(
        "Container built: {} step handlers, worker concurrency={}, re-drive lease={}s.",
        len(handlers),
        settings.worker_concurrency_limit,
        settings.redrive_lease_seconds,
    )

    return Container(
        workflow_service=WorkflowService(workflows),
        run_service=WorkflowRunService(runs, workflows),
        callback_service=RunCallbackService(runs),
        worker=worker,
        health_check=PostgresHealthCheck(session_factory),
    )
