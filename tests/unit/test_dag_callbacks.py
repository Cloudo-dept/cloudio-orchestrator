"""The DAG-side callbacks in ``airflow/dags/cloudio_callbacks.py``.

The module is loaded by path: it ships with the DAGs (Airflow puts its own dags folder on
sys.path), not with the orchestrator package. It is standard-library only precisely so it can be
driven here, without an Airflow worker — the fakes below stand in for the task instance and
dag_run objects Airflow passes in the callback context.
"""

import importlib.util
import json
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

MODULE_PATH = Path(__file__).parents[2] / "airflow" / "dags" / "cloudio_callbacks.py"


def _load() -> ModuleType:
    spec = importlib.util.spec_from_file_location("cloudio_callbacks", MODULE_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


callbacks = _load()


class FakeTaskInstance:
    """Records what a callback pushes; ``explode`` models an XCom push that fails."""

    def __init__(self, explode: bool = False) -> None:
        self.pushed: dict[str, str] = {}
        self.explode = explode

    def xcom_push(self, key: str, value: str) -> None:
        if self.explode:
            raise RuntimeError("api server unreachable")
        self.pushed[key] = value


def published(task_instance: FakeTaskInstance) -> dict[str, Any]:
    payload: dict[str, Any] = json.loads(task_instance.pushed[callbacks.EXCEPTION_XCOM_KEY])
    return payload


def test_publishes_message_and_type_under_the_key_the_adapter_reads() -> None:
    ti = FakeTaskInstance()
    callbacks.exception_callback("netops")({"ti": ti, "exception": RuntimeError("quota exceeded")})
    assert published(ti) == {
        "message": "quota exceeded",
        "exception": "RuntimeError",
        "responsible_group": "netops",
    }


def test_cloudio_exception_picks_the_group_over_the_callback_default() -> None:
    ti = FakeTaskInstance()
    error = callbacks.TaskException("no capacity", responsible_group="storage")
    callbacks.exception_callback("netops")({"ti": ti, "exception": error})
    assert published(ti)["responsible_group"] == "storage"


@pytest.mark.parametrize(
    "exception_name",
    ["ValidationException", "InfraPrecheckException", "TaskException"],
)
def test_each_exception_publishes_its_own_class_name(exception_name: str) -> None:
    # The class name is the contract: it is what the orchestrator classifies the failure by.
    ti = FakeTaskInstance()
    error = getattr(callbacks, exception_name)("nope")
    callbacks.exception_callback("netops")({"ti": ti, "exception": error})
    assert published(ti) == {
        "message": "nope",
        "exception": exception_name,
        "responsible_group": "netops",
    }


def test_group_is_none_when_neither_side_names_one() -> None:
    ti = FakeTaskInstance()  # → the orchestrator's default incident team owns it
    callbacks.exception_callback()({"ti": ti, "exception": ValueError("boom")})
    assert published(ti)["responsible_group"] is None


def test_failure_without_an_exception_still_publishes_a_message() -> None:
    ti = FakeTaskInstance()
    callbacks.exception_callback("netops")({"task_instance": ti})  # e.g. a task killed by a zombie
    assert published(ti)["message"] and published(ti)["exception"] is None


def test_dag_level_context_is_a_no_op() -> None:
    callbacks.exception_callback("netops")({"dag_run": SimpleNamespace(run_id="r-1")})  # no ti


def test_push_failure_never_masks_the_task_failure() -> None:
    callbacks.exception_callback("netops")(
        {"ti": FakeTaskInstance(explode=True), "exception": RuntimeError("boom")}
    )


def test_notify_posts_the_dag_run_id(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ORCHESTRATOR_BASE_URL", "http://orchestrator:8000/")
    sent: list[Any] = []
    monkeypatch.setattr(callbacks.urllib.request, "urlopen", lambda req, timeout: sent.append(req))
    callbacks.notify_orchestrator({"dag_run": SimpleNamespace(run_id="run-1")})
    assert sent[0].full_url == "http://orchestrator:8000" + callbacks.CALLBACK_PATH
    assert json.loads(sent[0].data) == {"engine_run_id": "run-1"}


def test_notify_is_a_no_op_without_a_base_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ORCHESTRATOR_BASE_URL", raising=False)
    monkeypatch.setattr(
        callbacks.urllib.request, "urlopen", lambda *a, **k: pytest.fail("posted anyway")
    )
    callbacks.notify_orchestrator({"dag_run": SimpleNamespace(run_id="run-1")})


def test_notify_swallows_a_failed_post(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ORCHESTRATOR_BASE_URL", "http://orchestrator:8000")

    def _boom(req: Any, timeout: float) -> None:
        raise OSError("connection refused")

    monkeypatch.setattr(callbacks.urllib.request, "urlopen", _boom)
    # Swallowed: the orchestrator polls the run to completion anyway.
    callbacks.notify_orchestrator({"dag_run": SimpleNamespace(run_id="run-1")})
