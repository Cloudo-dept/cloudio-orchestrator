"""Dev DAG: `provision_vm` — mimics a resource-provisioning engine run.

It generates a random vendor_id and pushes it to XCom under the key ``final_vendor_id`` — the
same key the orchestrator's engine adapter reads back (``get_output``) to learn which resource the
engine actually provisioned. Register a workflow with ``automation_id="provision_vm"`` to use it.

Both orchestrator hooks come from ``cloudio_callbacks`` (read its docstring before writing a DAG of
your own): the DAG wakes the waiting run on either outcome, and the task publishes its exception —
message plus responsible group — for the incident the orchestrator opens when the run fails. Set
``ORCHESTRATOR_BASE_URL`` (e.g. http://orchestrator:8000) to enable the wake-early callback; unset
= no callback.

Trigger it with ``conf={"fail": "validation" | "precheck" | "task"}`` to make the task raise that
kind of exception — the way to exercise each failure path end to end (validation/precheck: ticket
closed, no incident, no retry; task: retried, then an incident routed to `netops`). ``true`` is
accepted as a synonym for ``"task"``.
"""

from __future__ import annotations

import random
import string
from datetime import datetime

from airflow.decorators import dag, task
from airflow.sdk import get_current_context
from cloudio_callbacks import (
    CloudIOException,
    InfraPrecheckException,
    TaskException,
    ValidationException,
    exception_callback,
    notify_orchestrator,
)

FINAL_VENDOR_ID_KEY = "final_vendor_id"
RESPONSIBLE_GROUP = "storage"  # owns provisioning failures; maps to a ServiceNow assignment group

# Dev knob: conf["fail"] -> the exception the task raises (True == "task", the retried kind).
FAILURES: dict[str, type[CloudIOException]] = {
    "validation": ValidationException,
    "precheck": InfraPrecheckException,
    "task": TaskException,
}


@dag(
    dag_id="provision_vm",
    schedule=None,  # triggered on demand by the orchestrator
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["cloudio", "dev"],
    # Wake the waiting orchestrator run early on either outcome; it re-polls the real status.
    on_success_callback=notify_orchestrator,
    on_failure_callback=notify_orchestrator,
    # Every task publishes its exception where the orchestrator's get_failure looks for it.
    default_args={"on_failure_callback": exception_callback(RESPONSIBLE_GROUP)},
)
def provision_vm() -> None:
    @task
    def provision() -> str:
        context = get_current_context()
        fail_as = context["dag_run"].conf.get("fail")
        if fail_as:  # dev knob: exercise one of the failure paths
            failure = FAILURES.get("task" if fail_as is True else str(fail_as), TaskException)
            raise failure(
                f"Simulated {failure.__name__} (conf.fail={fail_as!r}).",
                responsible_group=RESPONSIBLE_GROUP,
            )
        vendor_id = "vm-" + "".join(random.choices(string.ascii_lowercase + string.digits, k=8))
        # Publish under the exact key the adapter's get_output() probes for.
        context["ti"].xcom_push(key=FINAL_VENDOR_ID_KEY, value=vendor_id)
        return vendor_id

    provision()


provision_vm()
