"""Dev DAG: `provision_vm` — mimics a resource-provisioning engine run.

It generates a random vendor_id and pushes it to XCom under the key ``final_vendor_id`` — the
same key the orchestrator's engine adapter reads back (``get_output``) to learn which resource the
engine actually provisioned. Register a workflow with ``automation_id="provision_vm"`` to use it.
"""

from __future__ import annotations

import random
import string
from datetime import datetime

from airflow.decorators import dag, task
from airflow.sdk import get_current_context

FINAL_VENDOR_ID_KEY = "final_vendor_id"


@dag(
    dag_id="provision_vm",
    schedule=None,  # triggered on demand by the orchestrator
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["cloudio", "dev"],
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
