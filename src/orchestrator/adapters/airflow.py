"""Airflow workflow-engine adapter — REST API v2, token auth, verify=False per the plugin spec."""

import json
from typing import Any

import httpx

from orchestrator.domain import EngineFailure, EngineRunStatus, FailureKind
from orchestrator.ports import WorkflowEngineClient

# The exception classes a CloudIO DAG raises (airflow/dags/cloudio_callbacks.py), mapped to the
# domain classification. This is the whole reason DAG vocabulary stops here: only FailureKind
# crosses the port. Matching is by exact class name — the payload carries a name, not a type — so a
# DAG that defines its own ValidationException instead of importing ours still classifies.
_FAILURE_KINDS: dict[str, FailureKind] = {
    "ValidationException": FailureKind.VALIDATION,
    "InfraPrecheckException": FailureKind.INFRA_PRECHECK,
    "TaskException": FailureKind.TASK,
}


def _failure_from_xcom(failed_task: str, body: Any) -> EngineFailure:
    """Turn the exception XCom a DAG's failure callback published into typed failure detail.

    The payload is ``{message, exception, responsible_group}`` (see ``airflow/dags/
    cloudio_callbacks.py``); `exception` is the class name that classifies the failure. Airflow
    wraps an XCom entry in ``{"key", "value", ...}`` and hands the value back stringified, so it
    normally arrives as the JSON string the callback pushed — hence the parse. Anything that is not
    that shape degrades to a bare failure rather than raising: the escalation still happens, it just
    loses the message and routing. An unrecognised (or missing) exception name stays TASK, so an
    unclassified DAG failure still opens an incident.
    """
    value: Any = body.get("value") if isinstance(body, dict) else None
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:  # a plain string value is the message itself
            return EngineFailure(failed_task=failed_task, detail=value)
    if not isinstance(value, dict):
        return EngineFailure(failed_task=failed_task)
    raw_name = value.get("exception")
    exception_name = raw_name if isinstance(raw_name, str) else None
    return EngineFailure(
        failed_task=failed_task,
        responsible_group=value.get("responsible_group"),
        detail=value.get("message"),
        exception_name=exception_name,
        kind=_FAILURE_KINDS.get(exception_name or "", FailureKind.TASK),
    )


class AirflowWorkflowEngineClient(WorkflowEngineClient):
    """Auth uses a bearer token from /auth/token, cached and refreshed on 401.
    automation_id == DAG id; the engine run id == dag_run_id.
    """

    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        timeout: float = 10.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._base = base_url.rstrip("/")
        self._username = username
        self._password = password
        self._timeout = timeout
        self._token: str | None = None
        self._transport = transport  # test seam (11-testing); None in prod

    def _client(self, token: str | None) -> httpx.AsyncClient:
        headers = {"Accept": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return httpx.AsyncClient(
            base_url=self._base,
            timeout=self._timeout,
            verify=False,
            headers=headers,
            transport=self._transport,
        )

    async def _authenticate(self) -> str:
        async with self._client(None) as client:
            resp = await client.post(
                "/auth/token", json={"username": self._username, "password": self._password}
            )
            resp.raise_for_status()
            data = resp.json()
            token: str = data.get("access_token") or data["token"]
            self._token = token
            return token

    async def _request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        token = self._token or await self._authenticate()
        async with self._client(token) as client:
            resp = await client.request(method, path, **kwargs)
        if resp.status_code == 401:  # token expired -> re-auth once
            token = await self._authenticate()
            async with self._client(token) as client:
                resp = await client.request(method, path, **kwargs)
        return resp

    async def trigger_workflow(
        self, automation_id: str, params: dict[str, Any], idempotency_key: str
    ) -> str:
        # dag_run_id doubles as the at-least-once idempotency key. logical_date is a required
        # field in the v2 API but null for an on-demand run (it has no scheduled interval).
        resp = await self._request(
            "POST",
            f"/api/v2/dags/{automation_id}/dagRuns",
            json={"dag_run_id": idempotency_key, "conf": params, "logical_date": None},
        )
        if resp.status_code == 409:  # duplicate dag_run_id -> already triggered
            return idempotency_key
        if resp.status_code == 404:
            raise RuntimeError(f"DAG '{automation_id}' not found.")
        resp.raise_for_status()
        run_id: str = resp.json()["dag_run_id"]
        return run_id

    async def query_run_status(self, automation_id: str, run_id: str) -> EngineRunStatus:
        resp = await self._request("GET", f"/api/v2/dags/{automation_id}/dagRuns/{run_id}")
        resp.raise_for_status()
        state = resp.json().get("state")  # queued | running | success | failed
        return {"success": EngineRunStatus.SUCCESS, "failed": EngineRunStatus.FAILED}.get(
            state, EngineRunStatus.IN_PROGRESS
        )

    async def get_failure(self, automation_id: str, run_id: str) -> EngineFailure:
        # Airflow needs two calls (failed task instances, then the task's exception XCom);
        # the port exposes ONE typed result — the two-call dance is an Airflow detail.
        resp = await self._request(
            "GET",
            f"/api/v2/dags/{automation_id}/dagRuns/{run_id}/taskInstances",
            params={"state": "failed"},
        )
        resp.raise_for_status()
        tasks = [ti["task_id"] for ti in resp.json().get("task_instances", [])]
        if not tasks:
            return EngineFailure()
        exc = await self._request(
            "GET",
            f"/api/v2/dags/{automation_id}/dagRuns/{run_id}/taskInstances/{tasks[0]}"
            f"/xcomEntries/exception_type",
        )
        if exc.status_code == 404:  # the task died before its failure callback published anything
            return EngineFailure(failed_task=tasks[0])
        exc.raise_for_status()
        return _failure_from_xcom(tasks[0], exc.json())

    async def get_output(self, automation_id: str, run_id: str, key: str) -> str | None:
        # XComs live on task instances; probe each task of the run for an entry named `key`.
        resp = await self._request(
            "GET", f"/api/v2/dags/{automation_id}/dagRuns/{run_id}/taskInstances"
        )
        resp.raise_for_status()
        for ti in resp.json().get("task_instances", []):
            entry = await self._request(
                "GET",
                f"/api/v2/dags/{automation_id}/dagRuns/{run_id}"
                f"/taskInstances/{ti['task_id']}/xcomEntries/{key}",
            )
            if entry.status_code == 404:  # this task did not push that key
                continue
            entry.raise_for_status()
            value = entry.json().get("value")
            if value is not None:
                return str(value)
        return None
