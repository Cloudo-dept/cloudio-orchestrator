"""The failure model: on permanent step failure, open an Incident and note the RITM. No rollback.

Across a retry the incident is a *per-problem* record, not a per-failure one:

* the **same** step failing again gets a comment on the incident that is already open — a retry of
  a problem that is still the same problem must not litter the queue with duplicate incidents;
* a **different** step failing means the retry got the run past what that incident was raised for,
  so it is closed as resolved-by-retry and a new one is opened for the new problem.
"""

from loguru import logger

from orchestrator.domain import RunState, StepName, TicketRef, WorkflowRun
from orchestrator.ports import TicketSystemClient


def _failure_fields(state: RunState) -> dict[str, str | None]:
    """The routing detail an incident carries about *this* failure, provider-neutral. Derived in
    one place so opening an incident and commenting on it can never describe it differently."""
    failure = state.engine_failure
    return {
        # only a DAG-run failure names a flow + task; anything else leaves both unset
        "flow_type": state.workflow.automation_id if failure and failure.failed_task else None,
        "failed_task": failure.failed_task if failure else None,
    }


def _label(step: StepName | None) -> str:
    """How a step is named in operator-facing text: the wire value ('running_engine'), never the
    enum's repr, which is what an f-string of a str-Enum member would give."""
    return f"'{step.value}'" if step is not None else "an unnamed step"


class FailureEscalator:
    """On permanent step failure: open an Incident to the responsible group (default team when
    unknown) and note it on the RITM if one exists. Runs when the run transitions to FAILED —
    there is no rollback afterwards. A run that is retried and fails again comes back here, and
    the incident already on the run decides whether to comment, or to close and re-raise."""

    def __init__(self, ticket_client: TicketSystemClient, default_team: str) -> None:
        self.ticket = ticket_client
        self.default_team = default_team

    async def escalate(self, run: WorkflowRun, error: Exception) -> None:
        st = run.run_state
        step = StepName(run.current_step) if run.current_step else None
        detail = f"{type(error).__name__}: {error}"  # exception type + message → incident note

        if st.incident is not None and st.incident_step == step:
            await self._comment_on_open_incident(run, step, detail)
            return
        if st.incident is not None:
            await self._close_superseded_incident(run, step)
        await self._open_incident(run, step, detail)

    async def _comment_on_open_incident(
        self, run: WorkflowRun, step: StepName | None, detail: str
    ) -> None:
        """The step this incident was raised for failed again — record it there, no duplicate."""
        st = run.run_state
        assert st.incident is not None
        note = (
            f"Run {run.run_id} was retried (retry #{st.manual_retries}) and failed again at "
            f"step {_label(step)}: {detail}"
        )
        logger.info(
            "Run {} failed again at {}; commenting on open incident {} instead of raising a new "
            "one.",
            run.run_id,
            step,
            st.incident.ticket_id,
        )
        try:
            # The repeat may have failed on a different task than the one this incident was
            # raised for — bring its failure fields up to the new state along with the note.
            await self.ticket.annotate_incident(st.incident, note, **_failure_fields(st))
        except Exception as e:  # never let escalation crash the worker
            logger.exception(
                "Failed to comment on incident {} for run {}: {}",
                st.incident.ticket_id,
                run.run_id,
                e,
            )

    async def _close_superseded_incident(self, run: WorkflowRun, step: StepName | None) -> None:
        """A different step fails now, so the retry resolved what this incident was about."""
        st = run.run_state
        assert st.incident is not None
        superseded, covered_step = st.incident, st.incident_step
        note = (
            f"Resolved by retrying: run {run.run_id} got past step {_label(covered_step)} on "
            f"retry #{st.manual_retries}. The run has since failed at step {_label(step)}, "
            f"which a new incident covers."
        )
        logger.info(
            "Run {} moved past {}; closing incident {} as resolved by the retry.",
            run.run_id,
            covered_step,
            superseded.ticket_id,
        )
        try:
            await self.ticket.close_incident(superseded, note)
        except Exception as e:  # a stuck close must not block the new incident
            logger.exception(
                "Failed to close superseded incident {} for run {}: {}",
                superseded.ticket_id,
                run.run_id,
                e,
            )

    async def _open_incident(self, run: WorkflowRun, step: StepName | None, detail: str) -> None:
        st = run.run_state
        failure = st.engine_failure
        if failure is not None:  # the failure came from the automation engine (a DAG failure)
            title = "Automation failure"
            group = failure.responsible_group or self.default_team
        else:  # the failure came from outside the engine → the default team owns it
            title = "Run execution failure"
            group = self.default_team
        logger.info(
            "Escalating run {} failure to group '{}' (opening incident).", run.run_id, group
        )
        try:
            inc = await self.ticket.open_incident(
                summary=title,
                requested_by=run.created_by,
                responsible_group=group,
                comment=detail,
                **_failure_fields(st),
            )
            st.incident, st.incident_step = inc, step  # what a later retry-failure looks at
            logger.info("Opened incident {} for run {}.", inc.ticket_id, run.run_id)
            if st.ticket is not None:
                await self._annotate_ticket(st.ticket, inc, detail)
        except Exception as e:  # never let escalation crash the worker
            logger.exception("Failed to escalate run {} failure: {}", run.run_id, e)

    async def _annotate_ticket(self, ticket: TicketRef, inc: TicketRef, detail: str) -> None:
        await self.ticket.annotate_ticket(
            ticket, f"Incident {inc.ticket_id} opened for failure: {detail}"
        )
        logger.debug("Annotated ticket {} with incident {}.", ticket.ticket_id, inc.ticket_id)
