"""The failure model: on permanent step failure, open an Incident and note the RITM. No rollback."""

from loguru import logger

from orchestrator.domain import WorkflowRun
from orchestrator.ports import TicketSystemClient


class FailureEscalator:
    """On permanent step failure: open an Incident to the responsible group (default team when
    unknown) and note it on the RITM if one exists. Runs once, when the run transitions to
    FAILED — there is no rollback afterwards."""

    def __init__(self, ticket_client: TicketSystemClient, default_team: str) -> None:
        self.ticket = ticket_client
        self.default_team = default_team

    async def escalate(self, run: WorkflowRun, error: Exception) -> None:
        st = run.run_state
        failure = st.engine_failure
        group = (
            failure.responsible_group
            if failure and failure.responsible_group
            else self.default_team
        )
        logger.info(
            "Escalating run {} failure to group '{}' (opening incident).", run.run_id, group
        )
        try:
            inc = await self.ticket.open_incident(
                summary=f"CloudIO run {run.run_id} failed at '{run.current_step}': {error}",
                requested_by=run.created_by,
                responsible_group=group,
                flow_type=st.workflow.automation_id if failure and failure.failed_task else None,
                failed_task=failure.failed_task if failure else None,
                comment=str(error),
            )
            st.incident_id = inc.ticket_id
            logger.info("Opened incident {} for run {}.", inc.ticket_id, run.run_id)
            if st.ticket is not None:
                await self.ticket.annotate_ticket(
                    st.ticket, f"Incident {inc.ticket_id} opened for failure: {error}"
                )
                logger.debug(
                    "Annotated ticket {} with incident {}.", st.ticket.ticket_id, inc.ticket_id
                )
        except Exception as e:  # never let escalation crash the worker
            logger.exception("Failed to escalate run {} failure: {}", run.run_id, e)
