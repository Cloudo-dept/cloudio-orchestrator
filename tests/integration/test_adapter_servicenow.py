"""Real ServiceNowTicketClient driven against the stateful ServiceNowMock."""

import httpx
import pytest

from orchestrator.adapters.servicenow import ServiceNowTicketClient
from orchestrator.domain import ApprovalStatus, TicketOutcome
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
    # The login was resolved to a sys_user sys_id before ordering.
    assert servicenow.ritms[-1].requested_for == servicenow.users["jdoe"]


async def test_open_ticket_falls_back_to_login_when_user_unknown(
    servicenow: ServiceNowMock, servicenow_client: ServiceNowTicketClient
) -> None:
    ref = await servicenow_client.open_ticket(
        template_id="cat-1", fields={}, requested_by="ghost", idempotency_key="k"
    )
    ritm = next(r for r in servicenow.ritms if r.sys_id == ref.native_id)
    assert ritm.requested_for == "ghost"  # no sys_user row → order with the raw login


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


async def test_close_ticket_unsuccessful_uses_the_incomplete_state(
    servicenow: ServiceNowMock, servicenow_client: ServiceNowTicketClient
) -> None:
    ref = await servicenow_client.open_ticket(
        template_id="cat-1", fields={}, requested_by="jdoe", idempotency_key="k"
    )
    await servicenow_client.close_ticket(
        ref, note="failed validation", outcome=TicketOutcome.UNSUCCESSFUL
    )
    ritm = next(r for r in servicenow.ritms if r.sys_id == ref.native_id)
    # Closed Incomplete, not Closed Complete — a failed request must not read as fulfilled.
    assert ritm.state == 4 and ritm.work_notes == ["failed validation"]


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


def group_lookups(servicenow: ServiceNowMock) -> int:
    return sum(1 for _, path in servicenow.requests if path.endswith("/sys_user_group"))


async def test_open_incident_routes_group_and_dag_fields(
    servicenow: ServiceNowMock, servicenow_client: ServiceNowTicketClient
) -> None:
    ref = await servicenow_client.open_incident(
        summary="Error in provision-vm automation",
        requested_by="jdoe",
        responsible_group="netops",
        flow_type="dag-x",
        failed_task="provision_vm",
        comment="RuntimeError: quota exceeded",
        description="Run 123 has failed with the following error:\nRuntimeError: quota exceeded",
    )
    assert ref.ticket_id.startswith("INC")
    body = servicenow.incidents[-1].body
    assert body["short_description"] == "Error in provision-vm automation"
    assert body["description"] == (
        "Run 123 has failed with the following error:\nRuntimeError: quota exceeded"
    )
    # A configured name is already a sys_id — assignment_group is never a name, and no lookup runs.
    assert body["assignment_group"] == "grpsys-netops"
    assert group_lookups(servicenow) == 0
    assert body["caller_id"] == servicenow.users["jdoe"]  # the login was resolved to a sys_id
    assert body["u_cloudio_flow_type"] == "dag-x"
    assert body["u_cloudio_failed_task"] == "provision_vm"
    assert body["work_notes"] == "RuntimeError: quota exceeded"  # comment → incident work note


async def test_open_incident_looks_up_an_unmapped_group_once(
    servicenow: ServiceNowMock, servicenow_client: ServiceNowTicketClient
) -> None:
    for _ in range(2):
        await servicenow_client.open_incident(
            summary="boom", requested_by="jdoe", responsible_group="CloudIO NetOps"
        )
    body = servicenow.incidents[-1].body
    assert body["assignment_group"] == servicenow.groups["CloudIO NetOps"]
    assert group_lookups(servicenow) == 1  # resolved once, then memoised
    assert "u_cloudio_flow_type" not in body  # no task → no DAG fields


async def test_open_incident_unknown_group_falls_back_to_the_default_team(
    servicenow: ServiceNowMock, servicenow_client: ServiceNowTicketClient
) -> None:
    await servicenow_client.open_incident(
        summary="boom", requested_by="jdoe", responsible_group="no-such-team"
    )
    # Nowhere to route it → the default incident team owns it, resolved the same way.
    assert servicenow.incidents[-1].body["assignment_group"] == servicenow.groups["cloudio"]


async def test_open_incident_omits_the_group_when_nothing_resolves(
    servicenow: ServiceNowMock, servicenow_client: ServiceNowTicketClient
) -> None:
    servicenow.groups.clear()  # neither the team nor the default team exists
    await servicenow_client.open_incident(
        summary="boom", requested_by="jdoe", responsible_group="no-such-team"
    )
    # ServiceNow's own triage beats a reference field holding a name that is not a sys_id.
    assert "assignment_group" not in servicenow.incidents[-1].body


async def test_open_incident_falls_back_to_the_login_for_an_unknown_caller(
    servicenow: ServiceNowMock, servicenow_client: ServiceNowTicketClient
) -> None:
    await servicenow_client.open_incident(
        summary="boom", requested_by="ghost", responsible_group="netops"
    )
    assert servicenow.incidents[-1].body["caller_id"] == "ghost"


async def test_open_ticket_assigns_the_ritm_to_the_approval_group(
    servicenow: ServiceNowMock, servicenow_client: ServiceNowTicketClient
) -> None:
    ref = await servicenow_client.open_ticket(
        template_id="cat-1",
        fields={},
        requested_by="jdoe",
        idempotency_key="k",
        approval_group="CloudIO NetOps",  # not pre-seeded → resolved against sys_user_group
    )
    ritm = next(r for r in servicenow.ritms if r.sys_id == ref.native_id)
    # Assigned in the same PATCH that tags the RITM — one call, not two.
    assert ritm.assignment_group == servicenow.groups["CloudIO NetOps"]
    assert ritm.correlation_id == "k"
    assert sum(1 for m, p in servicenow.requests if m == "PATCH") == 1


async def test_open_ticket_without_a_group_leaves_the_ritm_unassigned(
    servicenow: ServiceNowMock, servicenow_client: ServiceNowTicketClient
) -> None:
    ref = await servicenow_client.open_ticket(
        template_id="cat-1", fields={}, requested_by="jdoe", idempotency_key="k"
    )
    ritm = next(r for r in servicenow.ritms if r.sys_id == ref.native_id)
    assert ritm.assignment_group is None  # the catalog workflow keeps whatever it assigned
    assert group_lookups(servicenow) == 0


async def test_open_ticket_unknown_group_leaves_the_ritm_unassigned(
    servicenow: ServiceNowMock, servicenow_client: ServiceNowTicketClient
) -> None:
    ref = await servicenow_client.open_ticket(
        template_id="cat-1",
        fields={},
        requested_by="jdoe",
        idempotency_key="k",
        approval_group="no-such-team",
    )
    ritm = next(r for r in servicenow.ritms if r.sys_id == ref.native_id)
    # No default-team fallback here: an approval group is not an incident triage queue.
    assert ritm.assignment_group is None
    assert ritm.correlation_id == "k"  # the tag still landed


async def test_incident_open_5xx_is_surfaced(
    servicenow: ServiceNowMock, servicenow_client: ServiceNowTicketClient
) -> None:
    servicenow.overrides.append(Override(path_contains="/incident", status=500))
    with pytest.raises(httpx.HTTPStatusError):
        await servicenow_client.open_incident(
            summary="x", requested_by="jdoe", responsible_group="netops"
        )
