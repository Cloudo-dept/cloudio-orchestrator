"""The failure model: when a run ends badly, apply the failure policy and release the resource
it left in flight. No rollback."""

import logging

from orchestrator.domain import (
    EngineFailure,
    ResourceOperation,
    ResourceSpec,
    ResourceState,
    StepFailure,
    StepName,
    TicketOutcome,
    WorkflowRun,
)
from orchestrator.orchestration.failure_policy import FailurePolicy, policy_for
from orchestrator.orchestration.steps import resource_state_fields
from orchestrator.ports import ResourceManagerClient, TicketSystemClient

logger = logging.getLogger(__name__)


def incident_summary(run: WorkflowRun) -> str:
    """The incident's one-line title: which automation broke, and for which request. The RITM is
    what a responder searches by, so it belongs in the title — but a run can fail before its ticket
    exists (CREATE_TICKET itself failing), and "(None)" in a title helps nobody."""
    title = f"Error in {run.run_state.workflow.label} automation"
    ticket = run.run_state.ticket
    return f"{title} ({ticket.ticket_id})" if ticket is not None else title


def incident_description(run: WorkflowRun, detail: str) -> str:
    """The incident's body: which run broke and what it reported. The run id is what ties the
    incident back to the run store, so it leads."""
    return f"Run {run.run_id} has failed with the following error:\n{detail}"


def incident_flow_type(run: WorkflowRun, failure: EngineFailure | None) -> str:
    """Which flow broke, and where to go looking for it.

    A failure from inside the engine names the automation, prefixed by the engine that ran it
    (``Airflow:provision_vm``) — a bare DAG id tells a responder what broke but not which system to
    open. The prefix is derived from the run's engine type rather than written literally, both
    because engine names have no business being spelled out in orchestration code and so a second
    engine labels itself. Anything else names the workflow the run belongs to: a run that died
    before (or outside) its engine run has no automation to blame.
    """
    workflow = run.run_state.workflow
    if failure is not None and failure.failed_task:
        return f"{workflow.engine_type.value.capitalize()}:{workflow.automation_id}"
    return workflow.identifier


def incident_failed_task(run: WorkflowRun, failure: EngineFailure | None) -> str | None:
    """What broke: the engine task when the engine reported one, otherwise the orchestrator step
    that was running. `.value` because StepName is an Enum — str() on it yields "StepName.X"."""
    if failure is not None and failure.failed_task:
        return failure.failed_task
    return StepName(run.current_step).value if run.current_step else None


class FailureEscalator:
    """Runs once, when a run ends badly (FAILED or REJECTED), and never raises — escalation must
    not crash the worker. There is no rollback afterwards.

    A **classified** failure (``StepFailure`` — today only a DAG failure, whose exception the engine
    adapter classified) is escalated by its policy: an Incident to the responsible group when the
    policy asks for one, then the requester's ticket closed UNSUCCESSFUL with the policy's comment.
    Everything else — an adapter that exhausted its retries, a step that blew its deadline — keeps
    the older behaviour: an Incident to the default team and a work note, ticket left open. Those
    failures move onto the policy table by raising StepFailure with a kind; nothing else changes.

    Either way, a resource the run left in flight is then marked FAILED, so it stops advertising a
    run that is over. A **rejected** run (``reject``) escalates nothing — the ticket already carries
    the rejection — but releases its resource too. The ticket side and the resource side are
    guarded separately: one provider being down never costs the other its update.
    """

    def __init__(
        self,
        ticket_client: TicketSystemClient,
        resource_client: ResourceManagerClient,
        default_team: str,
    ) -> None:
        self.ticket = ticket_client
        self.resources = resource_client
        self.default_team = default_team

    async def escalate(self, run: WorkflowRun, error: Exception) -> None:
        try:
            if isinstance(error, StepFailure):
                await self._escalate_by_policy(run, error, policy_for(error.kind))
            else:
                await self._escalate_unclassified(run, error)
        except Exception as e:  # never let escalation crash the worker
            logger.exception("Failed to escalate run %s failure: %s", run.run_id, e)
        await self._mark_resource_failed(run)

    async def reject(self, run: WorkflowRun) -> None:
        """Release the resource a rejected request was put on. A CREATE's placeholder record is
        deleted — the resource was never provisioned; an UPDATE/DELETE leaves its resource READY,
        exactly as it was. Never raises."""
        resource = self._resource_in_flight(run)
        if resource is None:
            return
        try:
            if run.run_state.operation is ResourceOperation.CREATE:
                logger.info(
                    "Run %s rejected: deleting placeholder resource %s.",
                    run.run_id,
                    resource.vendor_id,
                )
                await self.resources.delete_resource(
                    resource.project_id, resource.resource_type, resource.vendor_id
                )
            else:
                logger.info(
                    "Run %s rejected: resource %s back to READY.", run.run_id, resource.vendor_id
                )
                await self.resources.update_resource(
                    resource.project_id,
                    resource.resource_type,
                    resource.vendor_id,
                    resource_state_fields(run, ResourceState.READY),
                )
        except Exception as e:  # never let escalation crash the worker
            logger.exception("Failed to release resource for rejected run %s: %s", run.run_id, e)

    async def _mark_resource_failed(self, run: WorkflowRun) -> None:
        resource = self._resource_in_flight(run)
        if resource is None:
            return
        try:
            await self.resources.update_resource(
                resource.project_id,
                resource.resource_type,
                resource.vendor_id,
                resource_state_fields(run, ResourceState.FAILED),
            )
            logger.info("Marked resource %s FAILED for run %s.", resource.vendor_id, run.run_id)
        except Exception as e:  # never let escalation crash the worker
            logger.exception("Failed to mark resource FAILED for run %s: %s", run.run_id, e)

    @staticmethod
    def _resource_in_flight(run: WorkflowRun) -> ResourceSpec | None:
        """The resource this run left in flight, if any: one it configured and did not finalize.
        Nothing else was touched — a run that ends before configure never reached the resource,
        and one that fails after finalize left it READY, which is the truth."""
        st = run.run_state
        if st.resource is None or not st.resource_configured or st.resource_finalized:
            return None
        return st.resource

    async def _escalate_by_policy(
        self, run: WorkflowRun, error: StepFailure, policy: FailurePolicy
    ) -> None:
        st = run.run_state
        failure = st.engine_failure
        logger.info(
            "Escalating run %s as a %s failure (incident=%s).",
            run.run_id,
            error.kind.value,
            policy.open_incident,
        )
        if policy.open_incident:
            group = (failure.responsible_group if failure else None) or self.default_team
            # What the failing task itself reported — the orchestrator's own wrapper message says
            # nothing a responder can act on. Falls back to it when the run published no detail.
            detail = str(error)
            if failure is not None and failure.detail:
                detail = f"{failure.exception_name or 'Failure'}: {failure.detail}"
            inc = await self.ticket.open_incident(
                summary=incident_summary(run),
                requested_by=run.created_by,
                responsible_group=group,
                flow_type=incident_flow_type(run, failure),
                failed_task=incident_failed_task(run, failure),
                comment=detail,
                description=incident_description(run, detail),
            )
            st.incident_id = inc.ticket_id
            # The team we *asked* for. Where it actually landed is the adapter's business — it
            # resolves the name and may fall back, and it logs that; saying "group 'x'" here when
            # the adapter routed elsewhere is how you end up chasing the wrong queue.
            logger.info(
                "Opened incident %s for run %s (requested team '%s').",
                inc.ticket_id,
                run.run_id,
                group,
            )
        if st.ticket is None:  # failed before the ticket existed — nothing to tell the requester
            logger.info("Run %s has no ticket to close.", run.run_id)
            return
        note = policy.close_comment.format(incident_id=st.incident_id)
        await self.ticket.close_ticket(st.ticket, note=note, outcome=TicketOutcome.UNSUCCESSFUL)
        st.ticket_closed = True  # the CLOSE_TICKET step is never reached, but a re-drive might be
        logger.info("Closed ticket %s as unsuccessful: %s", st.ticket.ticket_id, note)

    async def _escalate_unclassified(self, run: WorkflowRun, error: Exception) -> None:
        st = run.run_state
        detail = f"{type(error).__name__}: {error}"  # exception type + message → incident note
        logger.info(
            "Escalating run %s failure to group '%s' (opening incident).",
            run.run_id,
            self.default_team,
        )
        inc = await self.ticket.open_incident(
            summary=incident_summary(run),
            requested_by=run.created_by,
            responsible_group=self.default_team,
            comment=detail,
            description=incident_description(run, detail),
            # No engine failure to name, so these describe the orchestrator's own work: the
            # workflow being run, and the step it was on.
            flow_type=incident_flow_type(run, None),
            failed_task=incident_failed_task(run, None),
        )
        st.incident_id = inc.ticket_id
        logger.info("Opened incident %s for run %s.", inc.ticket_id, run.run_id)
        if st.ticket is not None:
            await self.ticket.annotate_ticket(
                st.ticket, f"Incident {inc.ticket_id} opened for failure: {error}"
            )
            logger.debug(
                "Annotated ticket %s with incident %s.", st.ticket.ticket_id, inc.ticket_id
            )
