"""Real AirflowWorkflowEngineClient driven against the stateful AirflowMock."""

import httpx
import pytest

from orchestrator.adapters.airflow import AirflowWorkflowEngineClient
from orchestrator.domain import EngineRunStatus, FailureKind
from tests.mocks.airflow import AirflowMock
from tests.mocks.base import Override


async def test_trigger_returns_run_id_and_polls_success(
    airflow: AirflowMock, airflow_client: AirflowWorkflowEngineClient
) -> None:
    run_id = await airflow_client.trigger_workflow("dag-x", {"size": "L"}, "run-1")
    assert run_id == "run-1"
    assert airflow.runs["run-1"].conf == {"size": "L"}
    status = await airflow_client.query_run_status("dag-x", "run-1")
    assert status is EngineRunStatus.SUCCESS


async def test_in_progress_maps_to_in_progress(
    airflow: AirflowMock, airflow_client: AirflowWorkflowEngineClient
) -> None:
    airflow.default_state = "running"
    await airflow_client.trigger_workflow("dag-x", {}, "run-1")
    assert await airflow_client.query_run_status("dag-x", "run-1") is EngineRunStatus.IN_PROGRESS


async def test_duplicate_trigger_is_idempotent(
    airflow: AirflowMock, airflow_client: AirflowWorkflowEngineClient
) -> None:
    await airflow_client.trigger_workflow("dag-x", {}, "run-1")
    again = await airflow_client.trigger_workflow("dag-x", {}, "run-1")  # 409 → already triggered
    assert again == "run-1"
    assert len(airflow.runs) == 1


async def test_trigger_unknown_dag_raises(
    airflow: AirflowMock, airflow_client: AirflowWorkflowEngineClient
) -> None:
    airflow.unknown_dags.add("ghost")
    with pytest.raises(RuntimeError, match="not found"):
        await airflow_client.trigger_workflow("ghost", {}, "run-1")


async def test_get_failure_returns_typed_group(
    airflow: AirflowMock, airflow_client: AirflowWorkflowEngineClient
) -> None:
    await airflow_client.trigger_workflow("dag-x", {}, "run-1")
    airflow.fail("run-1", task="provision_vm", responsible_group="netops", message="quota")
    failure = await airflow_client.get_failure("dag-x", "run-1")
    assert (failure.failed_task, failure.responsible_group, failure.detail) == (
        "provision_vm",
        "netops",
        "quota",
    )


@pytest.mark.parametrize(
    ("exception", "kind"),
    [
        ("ValidationException", FailureKind.VALIDATION),
        ("InfraPrecheckException", FailureKind.INFRA_PRECHECK),
        ("TaskException", FailureKind.TASK),
        ("ValueError", FailureKind.TASK),  # unclassified → the incident-opening default
    ],
)
async def test_get_failure_classifies_by_exception_name(
    exception: str,
    kind: FailureKind,
    airflow: AirflowMock,
    airflow_client: AirflowWorkflowEngineClient,
) -> None:
    await airflow_client.trigger_workflow("dag-x", {}, "run-1")
    airflow.fail("run-1", task="provision_vm", exception=exception, message="nope")
    failure = await airflow_client.get_failure("dag-x", "run-1")
    assert (failure.kind, failure.exception_name) == (kind, exception)


async def test_get_failure_empty_when_no_failed_task(
    airflow: AirflowMock, airflow_client: AirflowWorkflowEngineClient
) -> None:
    await airflow_client.trigger_workflow("dag-x", {}, "run-1")
    failure = await airflow_client.get_failure("dag-x", "run-1")  # no failed tasks recorded
    assert failure.failed_task is None and failure.responsible_group is None


async def test_token_refresh_on_401(
    airflow: AirflowMock, airflow_client: AirflowWorkflowEngineClient
) -> None:
    airflow.expire_token_once = True  # first authed call 401s
    await airflow_client.trigger_workflow("dag-x", {}, "run-1")  # transparently re-auths
    assert airflow.requests.count(("POST", "/auth/token")) == 2


async def test_status_5xx_is_surfaced(
    airflow: AirflowMock, airflow_client: AirflowWorkflowEngineClient
) -> None:
    await airflow_client.trigger_workflow("dag-x", {}, "run-1")
    airflow.overrides.append(Override(path_contains="/dagRuns/run-1", method="GET", status=500))
    with pytest.raises(httpx.HTTPStatusError):
        await airflow_client.query_run_status("dag-x", "run-1")


async def test_get_output_reads_published_xcom(
    airflow: AirflowMock, airflow_client: AirflowWorkflowEngineClient
) -> None:
    await airflow_client.trigger_workflow("dag-x", {}, "run-1")
    airflow.output("run-1", "final_vendor_id", "vm-engine-99")
    assert await airflow_client.get_output("dag-x", "run-1", "final_vendor_id") == "vm-engine-99"


async def test_get_output_none_when_absent(
    airflow: AirflowMock, airflow_client: AirflowWorkflowEngineClient
) -> None:
    await airflow_client.trigger_workflow("dag-x", {}, "run-1")
    assert await airflow_client.get_output("dag-x", "run-1", "final_vendor_id") is None


async def test_get_failure_without_exception_xcom_keeps_the_failed_task(
    airflow: AirflowMock, airflow_client: AirflowWorkflowEngineClient
) -> None:
    await airflow_client.trigger_workflow("dag-x", {}, "run-1")
    airflow.fail("run-1", task="provision", publish_exception=False)  # entry 404s
    failure = await airflow_client.get_failure("dag-x", "run-1")
    assert (failure.failed_task, failure.responsible_group, failure.detail) == (
        "provision",
        None,
        None,
    )
    assert failure.kind is FailureKind.TASK  # nothing to classify by → an incident is opened


async def test_get_failure_keeps_a_non_json_xcom_value_as_the_detail(
    airflow: AirflowMock, airflow_client: AirflowWorkflowEngineClient
) -> None:
    await airflow_client.trigger_workflow("dag-x", {}, "run-1")
    airflow.fail("run-1", task="provision")
    airflow.overrides.append(
        Override(  # a DAG that pushed a bare string instead of the documented payload
            path_contains="/xcomEntries/exception_type",
            method="GET",
            status=200,
            json={"key": "exception_type", "value": "disk quota exceeded"},
        )
    )
    failure = await airflow_client.get_failure("dag-x", "run-1")
    assert (failure.failed_task, failure.detail) == ("provision", "disk quota exceeded")


async def test_rolled_back_run_is_reported_as_failed(
    airflow: AirflowMock, airflow_client: AirflowWorkflowEngineClient
) -> None:
    # The bug this guards: rollbacks succeed, Airflow calls the run a success, and the
    # orchestrator would finalize the resource and close the RITM for work that was undone.
    await airflow_client.trigger_workflow("dag-x", {}, "run-1")
    airflow.rolled_back("run-1", failed_tasks={"provision_vm": "netops"})

    assert airflow.runs["run-1"].state == "success"  # what Airflow itself says
    assert await airflow_client.query_run_status("dag-x", "run-1") is EngineRunStatus.FAILED


async def test_a_genuine_success_is_still_a_success(
    airflow: AirflowMock, airflow_client: AirflowWorkflowEngineClient
) -> None:
    await airflow_client.trigger_workflow("dag-x", {}, "run-1")
    assert await airflow_client.query_run_status("dag-x", "run-1") is EngineRunStatus.SUCCESS


async def test_flow_failed_false_is_not_a_failure(
    airflow: AirflowMock, airflow_client: AirflowWorkflowEngineClient
) -> None:
    # A rollback branch that ran and found nothing wrong publishes the key with a falsey value.
    await airflow_client.trigger_workflow("dag-x", {}, "run-1")
    airflow.output("run-1", "flow_failed", "false")
    assert await airflow_client.query_run_status("dag-x", "run-1") is EngineRunStatus.SUCCESS


async def test_get_failure_reads_the_rollback_controllers_map(
    airflow: AirflowMock, airflow_client: AirflowWorkflowEngineClient
) -> None:
    await airflow_client.trigger_workflow("dag-x", {}, "run-1")
    airflow.rolled_back(
        "run-1",
        failed_tasks={"provision_vm": "netops", "attach_disk": "storage"},
        exception="ValidationException",
        message="region 'xx' unknown",
    )

    failure = await airflow_client.get_failure("dag-x", "run-1")

    # First entry only: one run, one incident. The group comes from the controller's map, while
    # the message and the classification still come from the task's own exception XCom.
    assert failure.failed_task == "provision_vm"
    assert failure.responsible_group == "netops"
    assert failure.detail == "region 'xx' unknown"
    assert failure.kind is FailureKind.VALIDATION


async def test_the_exceptions_own_group_outranks_the_controllers_map(
    airflow: AirflowMock, airflow_client: AirflowWorkflowEngineClient
) -> None:
    await airflow_client.trigger_workflow("dag-x", {}, "run-1")
    airflow.rolled_back("run-1", failed_tasks={"provision_vm": "netops"})
    # The task raised TaskException(responsible_group="storage") — more specific than the map.
    airflow.runs["run-1"].exception["responsible_group"] = "storage"

    failure = await airflow_client.get_failure("dag-x", "run-1")
    assert failure.responsible_group == "storage"
