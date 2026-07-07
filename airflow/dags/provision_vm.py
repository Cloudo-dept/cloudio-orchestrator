"""Dev DAG: `provision_vm` — mimics a resource-provisioning engine run.

It generates a random vendor_id and pushes it to XCom under the key ``final_vendor_id`` — the
same key the orchestrator's engine adapter reads back (``get_output``) to learn which resource the
engine actually provisioned. Register a workflow with ``automation_id="provision_vm"`` to use it.

On completion it best-effort notifies the orchestrator's engine-run callback so a run waiting on
this DAG is re-polled immediately instead of waiting out the poll interval. The callback carries
only the DAG run id (== the orchestrator's stored ``engine_run_id``); the orchestrator still polls
Airflow for the authoritative status, so a failed or missing callback changes nothing but latency.
Set ``ORCHESTRATOR_BASE_URL`` (e.g. http://orchestrator:8000) to enable it; unset = no callback.
"""

from __future__ import annotations

import os
import random
import string
import urllib.request
from datetime import datetime
from typing import Any

from airflow.decorators import dag, task
from airflow.sdk import get_current_context

FINAL_VENDOR_ID_KEY = "final_vendor_id"


def _notify_orchestrator(context: dict[str, Any]) -> None:
    """POST the DAG run id to the orchestrator's engine-run callback. Best-effort: any failure is
    swallowed (polling is the safety net), and it is a no-op when ORCHESTRATOR_BASE_URL is unset."""
    import json
    from contextlib import suppress

    base_url = os.environ.get("ORCHESTRATOR_BASE_URL")
    if not base_url:
        return
    engine_run_id = context["dag_run"].run_id
    body = json.dumps({"engine_run_id": engine_run_id}).encode()
    req = urllib.request.Request(  # noqa: S310 (fixed https/http orchestrator URL, not user input)
        f"{base_url.rstrip('/')}/api/v1/callbacks/engine-run",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with suppress(Exception):  # a lost callback just means the run advances on its next poll
        urllib.request.urlopen(req, timeout=5)  # noqa: S310


@dag(
    dag_id="provision_vm",
    schedule=None,  # triggered on demand by the orchestrator
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["cloudio", "dev"],
    # Wake the waiting orchestrator run early on either outcome; it re-polls the real status.
    on_success_callback=_notify_orchestrator,
    on_failure_callback=_notify_orchestrator,
)
def provision_vm() -> None:
    @task
    def provision() -> str:
        vendor_id = "vm-" + "".join(random.choices(string.ascii_lowercase + string.digits, k=8))
        # Publish under the exact key the adapter's get_output() probes for.
        get_current_context()["ti"].xcom_push(key=FINAL_VENDOR_ID_KEY, value=vendor_id)
        return vendor_id

    provision()


provision_vm()
