"""Real AirflowWorkflowEngineClient driven against the stateful AirflowMock."""

import httpx
import pytest

from orchestrator.adapters.airflow import AirflowWorkflowEngineClient
from orchestrator.domain import EngineRunStatus
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
