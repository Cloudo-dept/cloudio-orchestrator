"""Stateful Airflow mock: faithful routes + scenario knobs (fail / complete / token-expiry)."""

import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

from tests.mocks.base import Override, apply_overrides


@dataclass
class DagRun:
    dag_run_id: str
    conf: dict[str, Any]
    state: str = "success"  # queued | running | success | failed
    failed_task: str | None = None
    exception: dict[str, Any] = field(default_factory=dict)  # exception XCom; empty = none pushed
    xcoms: dict[str, str] = field(default_factory=dict)  # key -> value, on the "run_task" task


@dataclass
class AirflowMock:
    unknown_dags: set[str] = field(default_factory=set)  # DAGs that 404 on trigger
    runs: dict[str, DagRun] = field(default_factory=dict)  # keyed by dag_run_id
    default_state: str = "success"  # state a fresh trigger reports
    expire_token_once: bool = False  # force one 401 to exercise re-auth
    overrides: list[Override] = field(default_factory=list)
    requests: list[tuple[str, str]] = field(default_factory=list)
    _token_spent: bool = False

    # --- scenario knobs ---
    def complete(self, dag_run_id: str, state: str = "success") -> None:
        self.runs[dag_run_id].state = state

    def output(self, dag_run_id: str, key: str, value: str) -> None:
        """Publish a run output (XCom) the adapter's get_output can read back."""
        self.runs[dag_run_id].xcoms[key] = value

    def fail(
        self,
        dag_run_id: str,
        *,
        task: str,
        responsible_group: str | None = None,
        message: str = "boom",
        exception: str = "TaskException",
        publish_exception: bool = True,
    ) -> None:
        """Fail a run. `exception` is the class name the DAG raised — what the orchestrator
        classifies the failure by. publish_exception=False models a task that died without its
        failure callback publishing the exception XCom (the entry then 404s, as in real Airflow)."""
        r = self.runs[dag_run_id]
        r.state, r.failed_task = "failed", task
        r.exception = (
            {
                "message": message,
                "exception": exception,
                "responsible_group": responsible_group,
            }
            if publish_exception
            else {}
        )

    def rolled_back(
        self,
        dag_run_id: str,
        *,
        failed_tasks: dict[str, str],
        message: str = "boom",
        exception: str = "TaskException",
        publish_exception: bool = True,
    ) -> None:
        """A run whose rollback branch cleaned up after failed tasks: the DAG run reports
        **success**, while the tasks stay failed and the controller publishes its verdict
        (`flow_failed`) and what it found (`failed_tasks`: task_id -> responsible group)."""
        self.fail(
            dag_run_id,
            task=next(iter(failed_tasks)),
            responsible_group=None,  # the controller's map is what names the group here
            message=message,
            exception=exception,
            publish_exception=publish_exception,
        )
        r = self.runs[dag_run_id]
        r.state = "success"  # the rollbacks succeeded, so Airflow calls the run a success
        r.xcoms["flow_failed"] = "true"
        r.xcoms["failed_tasks"] = json.dumps(failed_tasks)

    @property
    def app(self) -> FastAPI:
        return _build(self)


def _build(mock: AirflowMock) -> FastAPI:
    app = FastAPI()

    @app.middleware("http")
    async def _record_override_auth(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        mock.requests.append((request.method, request.url.path))
        forced = apply_overrides(mock.overrides, request)
        if forced is not None:
            return forced
        if (
            mock.expire_token_once
            and not mock._token_spent
            and request.headers.get("authorization")
        ):
            mock._token_spent = True  # one 401 → adapter re-auths
            return JSONResponse({"detail": "expired"}, status_code=401)
        return await call_next(request)

    @app.post("/auth/token")
    async def token() -> dict[str, str]:
        return {"access_token": "test-token"}

    @app.post("/api/v2/dags/{dag_id}/dagRuns")
    async def trigger(dag_id: str, body: dict[str, Any]) -> JSONResponse:
        if dag_id in mock.unknown_dags:
            return JSONResponse({"detail": "not found"}, status_code=404)
        run_id = body["dag_run_id"]
        if run_id in mock.runs:  # duplicate dag_run_id
            return JSONResponse({"detail": "duplicate"}, status_code=409)
        mock.runs[run_id] = DagRun(
            dag_run_id=run_id, conf=body.get("conf", {}), state=mock.default_state
        )
        return JSONResponse({"dag_run_id": run_id})

    @app.get("/api/v2/dags/{dag_id}/dagRuns/{run_id}")
    async def status(dag_id: str, run_id: str) -> dict[str, Any]:
        return {"state": mock.runs[run_id].state}

    @app.get("/api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances")
    async def task_instances(dag_id: str, run_id: str, state: str | None = None) -> dict[str, Any]:
        r = mock.runs[run_id]
        if state == "failed":  # get_failure asks only for failed tasks
            return {"task_instances": [{"task_id": r.failed_task}] if r.failed_task else []}
        return {"task_instances": [{"task_id": "run_task"}]}  # the task that owns run xcoms

    # Specific route first so it wins over the generic {xcom_key} one below.
    @app.get(
        "/api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/xcomEntries/exception_type"
    )
    async def exception(dag_id: str, run_id: str, task_id: str) -> JSONResponse:
        exc = mock.runs[run_id].exception
        if not exc:  # the failing task published no exception XCom
            return JSONResponse({"detail": "not found"}, status_code=404)
        # Faithful shape: the entry is wrapped and the value comes back stringified — the DAG-side
        # failure callback pushes JSON, so the value is the JSON string the adapter parses.
        return JSONResponse({"key": "exception_type", "value": json.dumps(exc)})

    @app.get(
        "/api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/xcomEntries/{xcom_key}"
    )
    async def xcom(dag_id: str, run_id: str, task_id: str, xcom_key: str) -> JSONResponse:
        xcoms = mock.runs[run_id].xcoms
        if xcom_key not in xcoms:
            return JSONResponse({"detail": "not found"}, status_code=404)
        return JSONResponse({"key": xcom_key, "value": xcoms[xcom_key]})

    return app
