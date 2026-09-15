"""Run plans as data: the step order per run type, and the handler wiring."""

from collections.abc import Mapping

from orchestrator.domain import RunType, StepName, WorkflowEngineType
from orchestrator.orchestration.steps import (
    AwaitApprovalStep,
    CloseTicketStep,
    ConfigureResourceStep,
    CreateTicketStep,
    FinalizeResourceStep,
    RunEngineStep,
    StepHandler,
)
from orchestrator.ports import (
    ResourceManagerClient,
    TicketSystemClient,
    WorkflowEngineClient,
)

RUN_PLANS: dict[RunType, tuple[StepName, ...]] = {
    # Automation runs attach to the caller's pre-existing RITM (supplied at trigger time), so there
    # is no CREATE_TICKET step — the run only drives the engine and closes the ticket.
    RunType.AUTOMATION: (StepName.RUN_ENGINE, StepName.CLOSE_TICKET),
    # Resource runs put the request on its resource before waiting for approval, so the resource
    # shows the request under way from the moment it is made.
    RunType.RESOURCE: (
        StepName.CREATE_TICKET,
        StepName.CONFIGURE_RESOURCE,
        StepName.AWAIT_APPROVAL,
        StepName.RUN_ENGINE,
        StepName.FINALIZE_RESOURCE,
        StepName.CLOSE_TICKET,
    ),
}


def build_handlers(
    ticket_client: TicketSystemClient,
    resource_client: ResourceManagerClient,
    engines: Mapping[WorkflowEngineType, WorkflowEngineClient],
) -> dict[StepName, StepHandler]:
    return {
        StepName.CREATE_TICKET: CreateTicketStep(ticket_client),
        StepName.CONFIGURE_RESOURCE: ConfigureResourceStep(resource_client),
        StepName.AWAIT_APPROVAL: AwaitApprovalStep(ticket_client),
        StepName.RUN_ENGINE: RunEngineStep(engines),
        StepName.FINALIZE_RESOURCE: FinalizeResourceStep(resource_client),
        StepName.CLOSE_TICKET: CloseTicketStep(ticket_client),
    }
