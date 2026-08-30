"""Shared callbacks for DAGs the CloudIO orchestrator triggers.

Two hooks belong on such a DAG:

* ``notify_orchestrator`` — a **DAG-level** ``on_success_callback`` / ``on_failure_callback``. It
  best-effort POSTs the DAG run id to the wake-early callback endpoint so the waiting run re-polls
  immediately instead of sitting out its poll interval.
* ``exception_callback(...)`` — builds a **task-level** ``on_failure_callback`` that publishes the
  failing task's exception to the XCom key the orchestrator's engine adapter reads back
  (``get_failure`` → ``exception_type``), so the incident it opens carries the exception message and
  is routed to the group that owns the failure. It has to be a *task* callback: the orchestrator
  looks that XCom up on the failed task instance, and a DAG-level callback has no task instance to
  push from.

**Which exception you raise decides what the requester gets.** The published payload carries the
exception's *class name*, and the orchestrator maps that name to a failure policy:

===========================  ==========================  ====================================
Raise                        Incident                    The requester's ticket
===========================  ==========================  ====================================
``ValidationException``      none                        closed — validation error
``InfraPrecheckException``   none                        closed — validation error
``TaskException``            to the responsible group    closed — names the incident
anything else                to the responsible group    closed — names the incident
===========================  ==========================  ====================================

Validation and precheck failures are *not* retried (they would fail identically); a
``TaskException`` is retried before the incident opens. The exception's message becomes the
incident's work note, so make it say what went wrong. ``responsible_group`` names the group that
owns the failure and routes **the incident, and nothing else**; without one the callback's own group
is used, and failing that the orchestrator's default incident team. (It has no bearing on the
requester's ticket — that is assigned to the ``approval_group`` named when the run was triggered.)
Airflow accepts a *list* of callbacks, so DAG-specific code runs alongside these::

    from cloudio_callbacks import ValidationException, exception_callback, notify_orchestrator

    @dag(on_success_callback=notify_orchestrator, on_failure_callback=notify_orchestrator, ...)
    def provision_vm() -> None:
        @task(on_failure_callback=[exception_callback("netops"), page_the_on_call])
        def provision() -> str:
            raise ValidationException("region 'xx' is not a known region")

Neither callback is allowed to raise: a callback that blew up would mask the task's own failure, so
both swallow (and log) their errors. Polling is the safety net for the notification, and a missing
exception XCom only costs the incident its message and routing, not the escalation itself.

Deliberately standard-library only, so it stays importable (and readable) outside an Airflow worker.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.request
from collections.abc import Callable
from typing import Any

#: XCom key the orchestrator's Airflow adapter reads failure detail from (``get_failure``).
EXCEPTION_XCOM_KEY = "exception_type"

#: Wake-early endpoint on the orchestrator; enabled by ORCHESTRATOR_BASE_URL in Airflow's env.
CALLBACK_PATH = "/api/v1/callbacks/engine-run"
CALLBACK_TIMEOUT_SECONDS = 5.0

log = logging.getLogger(__name__)


class CloudIOException(Exception):
    """Base for the exceptions the orchestrator knows how to handle. Not raised directly.

    The **class name** is the contract — it is what the callback publishes and what the
    orchestrator maps to a failure policy — so these three names are fixed, and a subclass with a
    different name is handled like any unclassified error (incident to the responsible group).

    ``responsible_group`` picks who owns the failure; the orchestrator maps it to a ServiceNow
    assignment group, and an unregistered name is used verbatim.
    """

    def __init__(self, message: str, responsible_group: str | None = None) -> None:
        super().__init__(message)
        self.responsible_group = responsible_group


class ValidationException(CloudIOException):
    """The request is invalid — bad input, an object that is not shaped as expected. No incident:
    the requester's ticket is closed telling them it failed validation, and nothing is retried."""


class InfraPrecheckException(CloudIOException):
    """A precheck refused the request (capacity, policy, an unmet infrastructure precondition).
    Handled exactly like a validation failure: no incident, ticket closed, no retry."""


class TaskException(CloudIOException):
    """The work itself broke. Retried, then an incident is opened for the responsible group with
    this exception's message as its work note, and the ticket is closed naming that incident."""


def exception_callback(responsible_group: str | None = None) -> Callable[..., None]:
    """Build a task-level ``on_failure_callback`` that publishes the exception for the orchestrator.

    The payload is ``{message, exception, responsible_group}``, where ``exception`` is the class
    name the orchestrator classifies the failure by. ``responsible_group`` here is the fallback
    owner for exceptions that do not carry their own; leave it None to let the orchestrator's
    default incident team own them.
    """

    def _publish_exception(context: dict[str, Any], *_: Any) -> None:
        task_instance = context.get("ti") or context.get("task_instance")
        if task_instance is None:  # DAG-level context — no task instance to push an XCom onto
            log.warning("cloudio: exception callback has no task instance; nothing published.")
            return
        error = context.get("exception")
        payload = {
            "message": str(error) if error else "Task failed without an exception.",
            "exception": type(error).__name__ if isinstance(error, BaseException) else None,
            "responsible_group": getattr(error, "responsible_group", None) or responsible_group,
        }
        try:
            # Pushed as a JSON *string*: the REST API hands XCom values back stringified, so a
            # string round-trips unchanged and the adapter can json.loads it either way.
            task_instance.xcom_push(key=EXCEPTION_XCOM_KEY, value=json.dumps(payload))
        except Exception:  # a callback must never mask the task's own failure
            log.exception("cloudio: could not publish the exception XCom.")

    return _publish_exception


def notify_orchestrator(context: dict[str, Any], *_: Any) -> None:
    """POST the DAG run id to the orchestrator's engine-run callback (DAG-level, either outcome).

    Best-effort: any failure is swallowed (polling is the safety net), and it is a no-op when
    ORCHESTRATOR_BASE_URL is unset. The callback carries only the DAG run id (== the orchestrator's
    stored ``engine_run_id``) — the orchestrator still polls Airflow for the authoritative status.
    """
    base_url = os.environ.get("ORCHESTRATOR_BASE_URL")
    if not base_url:
        return
    body = json.dumps({"engine_run_id": context["dag_run"].run_id}).encode()
    request = urllib.request.Request(  # noqa: S310 (fixed orchestrator URL, not user input)
        f"{base_url.rstrip('/')}{CALLBACK_PATH}",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        urllib.request.urlopen(request, timeout=CALLBACK_TIMEOUT_SECONDS)  # noqa: S310
    except Exception:  # a lost callback only means the run advances on its next poll
        log.warning("cloudio: engine-run callback to %s failed; polling will catch up.", base_url)
