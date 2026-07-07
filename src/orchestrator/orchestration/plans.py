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
    RunType.AUTOMATION: (StepName.CREATE_TICKET, StepName.RUN_ENGINE, StepName.CLOSE_TICKET),
    RunType.RESOURCE: (
        StepName.CREATE_TICKET,
        StepName.AWAIT_APPROVAL,
        StepName.CONFIGURE_RESOURCE,
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
        StepName.AWAIT_APPROVAL: AwaitApprovalStep(ticket_client),
        StepName.CONFIGURE_RESOURCE: ConfigureResourceStep(resource_client),
        StepName.RUN_ENGINE: RunEngineStep(engines),
        StepName.FINALIZE_RESOURCE: FinalizeResourceStep(resource_client),
        StepName.CLOSE_TICKET: CloseTicketStep(ticket_client),
    }
