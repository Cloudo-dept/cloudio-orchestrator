"""Real ServiceNowTicketClient driven against the stateful ServiceNowMock."""

import httpx
import pytest

from orchestrator.adapters.servicenow import ServiceNowTicketClient
from orchestrator.domain import ApprovalStatus
from tests.mocks.base import Override
from tests.mocks.servicenow import ServiceNowMock


async def test_open_ticket_orders_then_returns_ritm(
    servicenow: ServiceNowMock, servicenow_client: ServiceNowTicketClient
) -> None:
    ref = await servicenow_client.open_ticket(
        template_id="cat-1",
        fields={"size": "L"},
        requested_by="jdoe",
        idempotency_key="run-1:creating_ticket:0",
    )
    assert ref.ticket_id.startswith("RITM") and ref.native_id
    # The created RITM was tagged with the idempotency key for later lookup.
    assert servicenow.ritms[-1].correlation_id == "run-1:creating_ticket:0"


async def test_open_ticket_is_idempotent_on_retry(
    servicenow: ServiceNowMock, servicenow_client: ServiceNowTicketClient
) -> None:
    servicenow.seed_ritm(correlation_id="run-1:creating_ticket:0")  # pretend already ordered
    ref = await servicenow_client.open_ticket(
        template_id="cat-1",
        fields={},
        requested_by="jdoe",
        idempotency_key="run-1:creating_ticket:0",
    )
    assert ref.ticket_id.startswith("RITM")
    assert not any(m == "POST" and "servicecatalog" in p for m, p in servicenow.requests)


async def test_close_ticket_sets_state_and_note(
    servicenow: ServiceNowMock, servicenow_client: ServiceNowTicketClient
) -> None:
    ref = await servicenow_client.open_ticket(
        template_id="cat-1", fields={}, requested_by="jdoe", idempotency_key="k"
    )
    await servicenow_client.close_ticket(ref, note="done")
    ritm = next(r for r in servicenow.ritms if r.sys_id == ref.native_id)
    assert ritm.state == 3 and ritm.work_notes == ["done"]


async def test_annotate_ticket_adds_note_only(
    servicenow: ServiceNowMock, servicenow_client: ServiceNowTicketClient
) -> None:
    ref = await servicenow_client.open_ticket(
        template_id="cat-1", fields={}, requested_by="jdoe", idempotency_key="k"
    )
    await servicenow_client.annotate_ticket(ref, "heads up")
    ritm = next(r for r in servicenow.ritms if r.sys_id == ref.native_id)
    assert ritm.work_notes == ["heads up"] and ritm.state is None


@pytest.mark.parametrize(
    ("field_value", "expected"),
    [
        ("approved", ApprovalStatus.APPROVED),
        ("rejected", ApprovalStatus.REJECTED),
        ("requested", ApprovalStatus.PENDING),
        ("", ApprovalStatus.PENDING),
    ],
)
async def test_get_approval_status_maps_ritm_field(
    servicenow: ServiceNowMock,
    servicenow_client: ServiceNowTicketClient,
    field_value: str,
    expected: ApprovalStatus,
) -> None:
    ref = await servicenow_client.open_ticket(
        template_id="cat-1", fields={}, requested_by="jdoe", idempotency_key="k"
    )
    next(r for r in servicenow.ritms if r.sys_id == ref.native_id).approval = field_value
    assert await servicenow_client.get_approval_status(ref) is expected


async def test_open_incident_routes_group_and_dag_fields(
    servicenow: ServiceNowMock, servicenow_client: ServiceNowTicketClient
) -> None:
    ref = await servicenow_client.open_incident(
        summary="boom",
        requested_by="jdoe",
        responsible_group="netops",
        flow_type="dag-x",
        failed_task="provision_vm",
    )
    assert ref.ticket_id.startswith("INC")
    body = servicenow.incidents[-1].body
    assert body["assignment_group"] == "CloudIO NetOps"  # mapped via responsible_groups
    assert body["u_cloudio_flow_type"] == "dag-x"
    assert body["u_cloudio_failed_task"] == "provision_vm"


async def test_open_incident_unmapped_group_used_verbatim(
    servicenow: ServiceNowMock, servicenow_client: ServiceNowTicketClient
) -> None:
    await servicenow_client.open_incident(
        summary="boom", requested_by="jdoe", responsible_group="cloudio"
    )
    body = servicenow.incidents[-1].body
    assert body["assignment_group"] == "cloudio"  # not in the map → used as-is
    assert "u_cloudio_flow_type" not in body  # no task → no DAG fields


async def test_incident_open_5xx_is_surfaced(
    servicenow: ServiceNowMock, servicenow_client: ServiceNowTicketClient
) -> None:
    servicenow.overrides.append(Override(path_contains="/incident", status=500))
    with pytest.raises(httpx.HTTPStatusError):
        await servicenow_client.open_incident(
            summary="x", requested_by="jdoe", responsible_group="netops"
        )
