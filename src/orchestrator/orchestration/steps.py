"""The step handlers.

Every handler is idempotent two ways:

1. **Typed state markers** — a re-driven step first checks whether its work already happened
   (``st.ticket``, ``resource_configured``, ``engine_run_id``, ``resource_finalized``,
   ``ticket_closed``) and short-circuits.
2. **Stable idempotency keys** — when the marker was not yet persisted (a crash between the side
   effect and ``save()``, or a retry), the side-effecting provider call carries a key stable per
   ``(run_id, step)``, so a provider that dedups on it returns the existing side effect instead of
   creating a duplicate (a second ticket, a second resource).

The one exception is the engine run id (``engine_run_key``): it is attempt-scoped so a *retry* of
RUN_ENGINE launches a fresh engine run rather than re-attaching to the failed one.
"""

import abc
import logging
from collections.abc import Mapping
from typing import Any

from orchestrator.domain import (
    ApprovalStatus,
    EngineRunStatus,
    FailureKind,
    ResourceOperation,
    RunRejected,
    RunState,
    StepFailure,
    StepName,
    StepResult,
    WorkflowEngineType,
    WorkflowRun,
)
from orchestrator.ports import (
    ResourceManagerClient,
    TicketSystemClient,
    WorkflowEngineClient,
)

logger = logging.getLogger(__name__)


# Named engine output (Airflow: an XCom) carrying the vendor id the run actually provisioned.
FINAL_VENDOR_ID_OUTPUT = "final_vendor_id"


def idem_key(run: WorkflowRun, step: StepName) -> str:
    """Stable per (run, step): a re-driven OR retried step reuses the same key, so a provider that
    dedups on it returns the existing side effect instead of creating a duplicate."""
    return f"{run.run_id}:{step.value}"


def engine_run_key(run: WorkflowRun) -> str:
    """The engine run id (Airflow: dag_run_id). Attempt-scoped, so a RUN_ENGINE *retry* triggers a
    fresh engine run instead of re-attaching to (and re-reading) the failed one."""
    attempt = run.run_state.step_attempts.get(StepName.RUN_ENGINE, 0)
    return f"{run.run_id}:{StepName.RUN_ENGINE.value}:{attempt}"


class StepHandler(abc.ABC):
    poll_interval_seconds: float = 15.0  # re-schedule delay while an async step is in progress
    max_step_duration_seconds: float = 3600  # overall wall-clock budget across polls

    @abc.abstractmethod
    async def execute(self, run: WorkflowRun) -> bool:
        """Return True when the step is complete, False when it must be polled again later.
        Raise to signal a failure (transient → retried, then FAILED once exhausted)."""

    def reset_for_retry(self, state: RunState) -> None:  # noqa: B027 (optional hook; default no-op)
        """Clear step-scoped fields so a retry starts clean."""


class CreateTicketStep(StepHandler):
    def __init__(self, ticket_client: TicketSystemClient) -> None:
        self.ticket_client = ticket_client

    async def execute(self, run: WorkflowRun) -> bool:
        st = run.run_state
        if st.ticket is not None:
            logger.debug(
                "Run %s: ticket %s already open; skipping.", run.run_id, st.ticket.ticket_id
            )
            return True
        logger.info(
            "Run %s: opening ticket from template %s.", run.run_id, st.workflow.ticket_template_id
        )
        st.ticket = await self.ticket_client.open_ticket(
            template_id=st.workflow.ticket_template_id,
            fields=st.ticket_params,
            requested_by=run.created_by,
            idempotency_key=idem_key(run, StepName.CREATE_TICKET),
            approval_group=st.approval_group,  # the team whose approval this request needs
        )
        logger.info("Run %s: opened ticket %s.", run.run_id, st.ticket.ticket_id)
        return True


class AwaitApprovalStep(StepHandler):
    """Wait for the RITM to be approved before any provisioning happens. Approval is a human
    action that can take days, so this never blocks the worker: it polls once, and while the
    ticket is still pending it returns False so the executor sets scheduled_at forward and a
    worker re-drives it later. A rejection ends the run terminally (no retry, no incident)."""

    poll_interval_seconds = 30.0  # a few minutes between approval checks
    max_step_duration_seconds = 7 * 24 * 3600  # ~7 days for a human to approve

    def __init__(self, ticket_client: TicketSystemClient) -> None:
        self.ticket_client = ticket_client

    async def execute(self, run: WorkflowRun) -> bool:
        st = run.run_state
        assert st.ticket is not None  # CREATE_TICKET ran first
        status = await self.ticket_client.get_approval_status(st.ticket)
        logger.info(
            "Run %s: approval status for ticket %s is %s.",
            run.run_id,
            st.ticket.ticket_id,
            status,
        )
        if status is ApprovalStatus.APPROVED:
            return True
        if status is ApprovalStatus.REJECTED:
            raise RunRejected(f"Ticket {st.ticket.ticket_id} was rejected.")
        return False  # pending → executor sets scheduled_at forward, worker re-drives


class ConfigureResourceStep(StepHandler):
    """Configure the resource for its operation (resource runs only), then hand off to the engine.

    A CREATE has no vendor id yet, so the run id becomes the resource's identity and a new
    record is created. An UPDATE/DELETE acts on a record that already exists, so we only mark
    the existing record in-progress. Either way FinalizeResourceStep clears in_progress once the
    engine is done. Idempotent on resource_configured."""

    def __init__(self, resource_client: ResourceManagerClient) -> None:
        self.resource_client = resource_client

    async def execute(self, run: WorkflowRun) -> bool:
        st = run.run_state
        if st.resource_configured:
            logger.debug("Run %s: resource already configured; skipping.", run.run_id)
            return True
        resource = st.resource
        assert resource is not None  # guaranteed by the trigger validation
        logger.info(
            "Run %s: configuring resource (operation=%s, type=%s, project=%s).",
            run.run_id,
            resource.operation,
            resource.resource_type,
            resource.project_id,
        )
        if resource.operation is ResourceOperation.CREATE:
            resource.vendor_id = str(run.run_id)  # the run id is the new resource's identity
            body = resource.model_dump(exclude={"project_id", "resource_type", "operation"}) | {
                "in_progress": True,
                "last_modified_by": run.created_by,
            }
            await self.resource_client.create_resource(
                project_id=resource.project_id,
                resource_type=resource.resource_type,
                body=body,
                idempotency_key=idem_key(run, StepName.CONFIGURE_RESOURCE),
            )
            logger.info("Run %s: created resource record %s.", run.run_id, resource.vendor_id)
        else:  # UPDATE / DELETE — the record already exists; just mark it in-progress.
            await self.resource_client.update_resource(
                resource.project_id,
                resource.resource_type,
                resource.vendor_id,
                {"in_progress": True},
            )
            logger.info(
                "Run %s: marked existing resource %s in-progress.", run.run_id, resource.vendor_id
            )
        st.resource_configured = True
        return True


class RunEngineStep(StepHandler):
    def __init__(self, engines: Mapping[WorkflowEngineType, WorkflowEngineClient]) -> None:
        self.engines = engines

    async def execute(self, run: WorkflowRun) -> bool:
        st = run.run_state
        client = self.engines[st.workflow.engine_type]

        if st.engine_run_id is None:  # not triggered yet → trigger, then poll later
            logger.info(
                "Run %s: triggering engine workflow '%s' on %s.",
                run.run_id,
                st.workflow.automation_id,
                st.workflow.engine_type,
            )
            st.engine_run_id = await client.trigger_workflow(
                automation_id=st.workflow.automation_id,
                params=st.workflow_params,
                idempotency_key=engine_run_key(run),  # attempt-scoped → fresh run per retry
            )
            logger.info(
                "Run %s: engine run %s triggered; will poll for completion.",
                run.run_id,
                st.engine_run_id,
            )
            return False

        status = await client.query_run_status(st.workflow.automation_id, st.engine_run_id)
        logger.info("Run %s: engine run %s status is %s.", run.run_id, st.engine_run_id, status)
        if status is EngineRunStatus.SUCCESS:
            await self._capture_final_vendor_id(run, client)
            return True
        if status is EngineRunStatus.FAILED:
            try:  # best-effort typed failure detail for the executor and the escalator
                st.engine_failure = await client.get_failure(
                    st.workflow.automation_id, st.engine_run_id
                )
                logger.warning(
                    "Run %s: engine failure detail — failed_task=%s, exception=%s, kind=%s.",
                    run.run_id,
                    st.engine_failure.failed_task,
                    st.engine_failure.exception_name,
                    st.engine_failure.kind.value,
                )
            except Exception as e:
                logger.warning("Could not fetch failure detail for run %s: %s", run.run_id, e)
            # Classified, so the failure policy decides whether this is worth another engine run
            # and what the requester is told. Unknown detail → TASK: retried, then an incident.
            kind = st.engine_failure.kind if st.engine_failure else FailureKind.TASK
            raise StepFailure(
                f"Engine run {st.engine_run_id} of '{st.workflow.automation_id}' failed.",
                kind=kind,
            )
        return False  # still running → poll again later

    async def _capture_final_vendor_id(
        self, run: WorkflowRun, client: WorkflowEngineClient
    ) -> None:
        # If the run published a `final_vendor_id` output, record it in this step's results so
        # finalize can re-key the placeholder record to the id the engine actually provisioned.
        st = run.run_state
        if st.resource is None or st.engine_run_id is None:
            return
        try:
            final_vendor_id = await client.get_output(
                st.workflow.automation_id, st.engine_run_id, FINAL_VENDOR_ID_OUTPUT
            )
        except Exception as e:  # optional enrichment — never fail a succeeded run over it
            logger.warning(
                "Could not read %s for run %s: %s", FINAL_VENDOR_ID_OUTPUT, run.run_id, e
            )
            return
        if final_vendor_id is not None:
            logger.info("Run %s: engine reported final vendor id %s.", run.run_id, final_vendor_id)
            st.step_results[StepName.RUN_ENGINE] = StepResult(final_vendor_id=final_vendor_id)

    def reset_for_retry(self, state: RunState) -> None:  # noqa: B027 (optional hook; default no-op)
        logger.debug("Resetting RUN_ENGINE state for retry (clearing engine_run_id).")
        state.engine_run_id = None
        state.engine_failure = None
        state.step_results.pop(StepName.RUN_ENGINE, None)


class FinalizeResourceStep(StepHandler):
    """Mark the created resource provisioned (resource runs only). Idempotent on
    resource_finalized; a no-op if the run has no resource."""

    def __init__(self, resource_client: ResourceManagerClient) -> None:
        self.resource_client = resource_client

    async def execute(self, run: WorkflowRun) -> bool:
        st = run.run_state
        if st.resource is None or st.resource_finalized:
            logger.debug("Run %s: no resource to finalize (or already done).", run.run_id)
            return True
        # Target the record where ConfigureResourceStep created/targeted it (resource.vendor_id —
        # the run id for a CREATE). If the engine reported the id it actually provisioned (the
        # RUN_ENGINE step result's final_vendor_id), record it on that record as we finalize — a
        # re-key from the run-id placeholder to the real vendor id.
        vendor_id = st.resource.vendor_id
        fields: dict[str, Any] = {"in_progress": False}  # done provisioning
        engine_result = st.step_results.get(StepName.RUN_ENGINE)
        engine_vendor_id = engine_result.final_vendor_id if engine_result else None
        if engine_vendor_id and engine_vendor_id != vendor_id:
            fields["vendor_id"] = engine_vendor_id
            logger.info(
                "Run %s: re-keying resource %s -> %s on finalize.",
                run.run_id,
                vendor_id,
                engine_vendor_id,
            )
        logger.info("Run %s: finalizing resource %s (in_progress=False).", run.run_id, vendor_id)
        await self.resource_client.update_resource(
            st.resource.project_id,
            st.resource.resource_type,
            vendor_id,
            fields,
        )
        st.resource_finalized = True
        return True


class CloseTicketStep(StepHandler):
    """Close out the RITM. Idempotent on ticket_closed."""

    def __init__(self, ticket_client: TicketSystemClient) -> None:
        self.ticket_client = ticket_client

    async def execute(self, run: WorkflowRun) -> bool:
        st = run.run_state
        if st.ticket_closed:
            logger.debug("Run %s: ticket already closed; skipping.", run.run_id)
            return True
        assert st.ticket is not None
        note = (
            "Resource provisioned; request closed."
            if st.resource is not None
            else "CloudIO automation completed."
        )
        logger.info("Run %s: closing ticket %s.", run.run_id, st.ticket.ticket_id)
        await self.ticket_client.close_ticket(st.ticket, note=note)
        st.ticket_closed = True
        return True
