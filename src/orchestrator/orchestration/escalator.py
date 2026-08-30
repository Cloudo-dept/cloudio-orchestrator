"""The failure model: on permanent step failure, apply the failure policy. No rollback."""

import logging

from orchestrator.domain import StepFailure, TicketOutcome, WorkflowRun
from orchestrator.orchestration.failure_policy import FailurePolicy, policy_for
from orchestrator.ports import TicketSystemClient

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


class FailureEscalator:
    """Runs once, when a run transitions to FAILED, and never raises — escalation must not crash
    the worker. There is no rollback afterwards.

    A **classified** failure (``StepFailure`` — today only a DAG failure, whose exception the engine
    adapter classified) is escalated by its policy: an Incident to the responsible group when the
    policy asks for one, then the requester's ticket closed UNSUCCESSFUL with the policy's comment.
    Everything else — an adapter that exhausted its retries, a step that blew its deadline — keeps
    the older behaviour: an Incident to the default team and a work note, ticket left open. Those
    failures move onto the policy table by raising StepFailure with a kind; nothing else changes.
    """

    def __init__(self, ticket_client: TicketSystemClient, default_team: str) -> None:
        self.ticket = ticket_client
        self.default_team = default_team

    async def escalate(self, run: WorkflowRun, error: Exception) -> None:
        try:
            if isinstance(error, StepFailure):
                await self._escalate_by_policy(run, error, policy_for(error.kind))
            else:
                await self._escalate_unclassified(run, error)
        except Exception as e:  # never let escalation crash the worker
            logger.exception("Failed to escalate run %s failure: %s", run.run_id, e)

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
                flow_type=st.workflow.automation_id if failure and failure.failed_task else None,
                failed_task=failure.failed_task if failure else None,
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
