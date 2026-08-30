*[← Index](README.md)*

# Orchestration — steps, run plans, executor, escalator

The run's step order is **data** (`RUN_PLANS`), what each kind of failure costs the requester is
data too (`FAILURE_POLICIES`), handlers are small classes behind one ABC, the executor is what a
worker invokes per run, and the escalator applies the failure model.
There is **no compensation** — permanent failure ends the run at `FAILED` and escalates.

Both flows start by creating the ticket. `ConfigureResourceStep` configures the resource for its operation — a
`create` provisions a new record (assigning the run id as its vendor id); an `update`/`delete`
just marks the existing record in-progress. Finalization is two independent steps:
`FinalizeResourceStep` clears in-progress once the engine is done (resource runs only), and the
shared `CloseTicketStep` closes the RITM:

- **automation**: `running_engine → closing_ticket` — attaches to the caller's pre-existing RITM
  (supplied at trigger time), so there is no `creating_ticket` step
- **resource**: `creating_ticket → configuring_resource → running_engine → finalizing_resource → closing_ticket`

## `orchestration/plans.py` — run plans as data

```python
from collections.abc import Mapping

from orchestrator.domain import RunType, StepName, WorkflowEngineType
from orchestrator.orchestration.steps import (ConfigureResourceStep, CloseTicketStep,
                                              CreateTicketStep, FinalizeResourceStep,
                                              RunEngineStep, StepHandler)
from orchestrator.ports import ResourceManagerClient, TicketSystemClient, WorkflowEngineClient

RUN_PLANS: dict[RunType, tuple[StepName, ...]] = {
    RunType.AUTOMATION: (StepName.RUN_ENGINE, StepName.CLOSE_TICKET),
    RunType.RESOURCE: (StepName.CREATE_TICKET, StepName.CONFIGURE_RESOURCE, StepName.RUN_ENGINE,
                       StepName.FINALIZE_RESOURCE, StepName.CLOSE_TICKET),
}


def build_handlers(
    ticket_client: TicketSystemClient,
    resource_client: ResourceManagerClient,
    engines: Mapping[WorkflowEngineType, WorkflowEngineClient],
) -> dict[StepName, StepHandler]:
    return {
        StepName.CREATE_TICKET: CreateTicketStep(ticket_client),
        StepName.CONFIGURE_RESOURCE: ConfigureResourceStep(resource_client),
        StepName.RUN_ENGINE: RunEngineStep(engines),
        StepName.FINALIZE_RESOURCE: FinalizeResourceStep(resource_client),
        StepName.CLOSE_TICKET: CloseTicketStep(ticket_client),
    }
```

## `orchestration/steps.py` — the step handlers

Every handler is idempotent on its typed state markers: a re-driven step first checks whether its
work already happened. Side-effecting calls carry an idempotency key, and there are two kinds.
Ticket and resource creation use `idem_key(run, step)` — `(run_id, step)`, stable across re-drives
*and* retries, so a provider that dedups on it returns the existing side effect instead of creating
a duplicate. The engine trigger uses `engine_run_key(run)` — `(run_id, running_engine, attempt)`,
deliberately attempt-scoped so a retry starts a fresh engine run instead of re-attaching to (and
re-reading) the failed one.

```python
import abc
import logging
from collections.abc import Mapping

from orchestrator.domain import (EngineRunStatus, StepName, TicketRef, WorkflowEngineType,
                                 WorkflowRun, RunState)
from orchestrator.ports import ResourceManagerClient, TicketSystemClient, WorkflowEngineClient

logger = logging.getLogger(__name__)

# Named engine output (Airflow: an XCom) carrying the vendor id the run actually provisioned.
FINAL_VENDOR_ID_OUTPUT = "final_vendor_id"


def idem_key(run: WorkflowRun, step: StepName) -> str:
    """Stable per (run, step): a re-driven OR retried step reuses the same key, so a provider that
    dedups on it returns the existing side effect instead of creating a duplicate."""
    return f"{run.run_id}:{step.value}"


def engine_run_key(run: WorkflowRun) -> str:
    """The engine run id (Airflow: dag_run_id). Attempt-scoped, so a RUN_ENGINE *retry* triggers a
    fresh engine run instead of re-attaching to (and re-reading) the failed one."""
    attempt = run.run_state.step_attempts.get(StepName.RUN_ENGINE, 0)
    return f"{run.run_id}:{StepName.RUN_ENGINE.value}:{attempt}"


class StepHandler(abc.ABC):
    poll_interval_seconds: float = 15.0      # re-schedule delay while an async step is in progress
    max_step_duration_seconds: float = 3600  # overall wall-clock budget across polls

    @abc.abstractmethod
    async def execute(self, run: WorkflowRun) -> bool:
        """Return True when the step is complete, False when it must be polled again later.
        Raise to signal a failure (transient → retried, then FAILED once exhausted)."""

    def reset_for_retry(self, state: RunState) -> None:
        """Clear step-scoped fields so a retry starts clean."""


class CreateTicketStep(StepHandler):
    def __init__(self, ticket_client: TicketSystemClient) -> None:
        self.ticket_client = ticket_client

    async def execute(self, run: WorkflowRun) -> bool:
        st = run.run_state
        if st.ticket is not None:
            return True
        st.ticket = await self.ticket_client.open_ticket(
            template_id=st.workflow.ticket_template_id,
            fields=st.ticket_params,
            requested_by=run.created_by,
            idempotency_key=idem_key(run, StepName.CREATE_TICKET),
            approval_group=st.approval_group)   # the team whose approval this request needs
        return True


class ConfigureResourceStep(StepHandler):
    def __init__(self, resource_client: ResourceManagerClient) -> None:
        self.resource_client = resource_client

    async def execute(self, run: WorkflowRun) -> bool:
        st = run.run_state
        if st.resource_configured:
            return True
        resource = st.resource
        assert resource is not None             # guaranteed by the trigger validation
        if resource.operation is ResourceOperation.CREATE:
            resource.vendor_id = str(run.run_id)  # the run id is the new resource's identity
            body = resource.model_dump(exclude={"project_id", "resource_type", "operation"}) | {
                "in_progress": True, "last_modified_by": run.created_by}
            await self.resource_client.create_resource(
                project_id=resource.project_id, resource_type=resource.resource_type,
                body=body, idempotency_key=idem_key(run, StepName.CONFIGURE_RESOURCE))
        else:                                   # update / delete — record exists; mark in-progress
            await self.resource_client.update_resource(
                resource.project_id, resource.resource_type, resource.vendor_id,
                {"in_progress": True})
        st.resource_configured = True
        return True


class RunEngineStep(StepHandler):
    def __init__(self, engines: Mapping[WorkflowEngineType, WorkflowEngineClient]) -> None:
        self.engines = engines

    async def execute(self, run: WorkflowRun) -> bool:
        st = run.run_state
        client = self.engines[st.workflow.engine_type]

        if st.engine_run_id is None:            # not triggered yet → trigger, then poll later
            st.engine_run_id = await client.trigger_workflow(
                automation_id=st.workflow.automation_id,
                params=st.workflow_params,
                idempotency_key=engine_run_key(run))  # attempt-scoped → fresh run per retry
            return False

        status = await client.query_run_status(st.workflow.automation_id, st.engine_run_id)
        if status is EngineRunStatus.SUCCESS:
            await self._capture_final_vendor_id(run, client)
            return True
        if status is EngineRunStatus.FAILED:
            try:                # best-effort typed detail for the executor and the escalator
                st.engine_failure = await client.get_failure(
                    st.workflow.automation_id, st.engine_run_id)
            except Exception as e:
                logger.warning("Could not fetch failure detail for run %s: %s", run.run_id, e)
            # Classified, so the failure policy decides whether this is worth another engine run
            # and what the requester is told. Unknown detail → TASK: retried, then an incident.
            kind = st.engine_failure.kind if st.engine_failure else FailureKind.TASK
            raise StepFailure(
                f"Engine run {st.engine_run_id} of '{st.workflow.automation_id}' failed.",
                kind=kind)
        return False                            # still running → poll again later

    async def _capture_final_vendor_id(self, run: WorkflowRun,
                                       client: WorkflowEngineClient) -> None:
        # If the run published a `final_vendor_id` output, record it under the resource's
        # data.vendor_id so finalize targets the resource the engine actually provisioned.
        st = run.run_state
        if st.resource is None or st.engine_run_id is None:
            return
        try:
            final_vendor_id = await client.get_output(
                st.workflow.automation_id, st.engine_run_id, FINAL_VENDOR_ID_OUTPUT)
        except Exception as e:                  # optional enrichment — never fail a succeeded run
            logger.warning("Could not read %s for run %s: %s",
                           FINAL_VENDOR_ID_OUTPUT, run.run_id, e)
            return
        if final_vendor_id is not None:
            st.resource.data["vendor_id"] = final_vendor_id

    def reset_for_retry(self, state: RunState) -> None:
        state.engine_run_id = None
        state.engine_failure = None


class FinalizeResourceStep(StepHandler):
    """Mark the created resource provisioned (resource runs only). Idempotent on
    resource_finalized; a no-op if the run has no resource."""

    def __init__(self, resource_client: ResourceManagerClient) -> None:
        self.resource_client = resource_client

    async def execute(self, run: WorkflowRun) -> bool:
        st = run.run_state
        if st.resource is None or st.resource_finalized:
            return True
        # Prefer the vendor id the engine reported (data.vendor_id, from the final_vendor_id
        # output); fall back to the caller-supplied vendor_id when the run produced none.
        vendor_id = st.resource.data.get("vendor_id") or st.resource.vendor_id
        await self.resource_client.update_resource(
            st.resource.project_id, st.resource.resource_type, vendor_id,
            {"in_progress": False})              # done provisioning
        st.resource_finalized = True
        return True


class CloseTicketStep(StepHandler):
    """Close out the RITM. Idempotent on ticket_closed."""

    def __init__(self, ticket_client: TicketSystemClient) -> None:
        self.ticket_client = ticket_client

    async def execute(self, run: WorkflowRun) -> bool:
        st = run.run_state
        if st.ticket_closed:
            return True
        assert st.ticket is not None
        note = ("Resource provisioned; request closed." if st.resource is not None
                else "CloudIO automation completed.")
        await self.ticket_client.close_ticket(st.ticket, note=note)
        st.ticket_closed = True
        return True
```

## `orchestration/failure_policy.py` — what a failure costs the requester, as data

Not every failure deserves an Incident. A DAG that rejects a malformed request has nothing for an
on-call engineer to do; a DAG whose provisioning call broke does. One row per `FailureKind`, read by
two consumers — the executor asks *may I retry this*, the escalator asks *incident? which ticket
comment?* — so adding a kind (or moving a non-engine failure onto the table later) is a row, not a
branch.

```python
class FailurePolicy(BaseModel):
    retryable: bool         # False → terminal on the first failure, no backoff, no second run
    open_incident: bool     # True → an Incident to the responsible group before closing the ticket
    close_comment: str      # note the ticket is closed with; may reference {incident_id}


VALIDATION_COMMENT = "Your request was closed due to a validation error"

FAILURE_POLICIES: dict[FailureKind, FailurePolicy] = {
    FailureKind.VALIDATION:     FailurePolicy(retryable=False, open_incident=False,
                                              close_comment=VALIDATION_COMMENT),
    FailureKind.INFRA_PRECHECK: FailurePolicy(retryable=False, open_incident=False,
                                              close_comment=VALIDATION_COMMENT),
    FailureKind.TASK:           FailurePolicy(retryable=True, open_incident=True,
                                              close_comment="An error occurred. "
                                                            "Incident {incident_id} was created"),
}


def policy_for(kind: FailureKind) -> FailurePolicy:
    """The policy for `kind`, defaulting to TASK's (incident + retries) for anything unmapped."""
    return FAILURE_POLICIES.get(kind, FAILURE_POLICIES[FailureKind.TASK])
```

The kind comes from the engine: a CloudIO DAG raises `ValidationException` /
`InfraPrecheckException` / `TaskException`, the DAG's failure callback publishes the class name, and
[`adapters/airflow.py`](06-external-clients.md) maps that name to a `FailureKind` — so DAG
vocabulary stops at the adapter and only the classification crosses the port. An unclassified
failure is a `TASK` failure: retried, then an Incident.

## `orchestration/escalator.py` — the failure model

Every incident it opens is titled and bodied the same way, so a responder sees which automation
broke and what it reported without opening the run store:

```python
def incident_summary(run: WorkflowRun) -> str:                       # short_description
    title = f"Error in {run.run_state.workflow.label} automation"     # name, else identifier
    ticket = run.run_state.ticket        # omitted when the run failed before its RITM existed
    return f"{title} ({ticket.ticket_id})" if ticket is not None else title


def incident_description(run: WorkflowRun, detail: str) -> str:      # description
    return f"Run {run.run_id} has failed with the following error:\n{detail}"
```

`detail` is the failing task's own exception (`"TaskException: quota exceeded"`), not the
orchestrator's wrapper message, and it is sent **twice** — as the description and as a work note.

A **classified** failure (`StepFailure`) is escalated by its policy; everything else — an adapter
that exhausted its retries, a step that blew its deadline — keeps the older behaviour (Incident to
the default team, work note, ticket left open). That is the only branch, and it is transitional:
those failures move onto the policy table by raising `StepFailure` with a kind.

```python
class FailureEscalator:
    """Runs once, when a run transitions to FAILED, and never raises — escalation must not crash
    the worker. There is no rollback afterwards."""

    def __init__(self, ticket_client: TicketSystemClient, default_team: str) -> None:
        self.ticket = ticket_client
        self.default_team = default_team

    async def escalate(self, run: WorkflowRun, error: Exception) -> None:
        try:
            if isinstance(error, StepFailure):
                await self._escalate_by_policy(run, error, policy_for(error.kind))
            else:
                await self._escalate_unclassified(run, error)
        except Exception as e:      # never let escalation crash the worker
            logger.exception("Failed to escalate run %s failure: %s", run.run_id, e)

    async def _escalate_by_policy(self, run: WorkflowRun, error: StepFailure,
                                  policy: FailurePolicy) -> None:
        st, failure = run.run_state, run.run_state.engine_failure
        if policy.open_incident:
            group = (failure.responsible_group if failure else None) or self.default_team
            # What the failing task reported — the orchestrator's own wrapper message says
            # nothing a responder can act on.
            detail = str(error)
            if failure is not None and failure.detail:
                detail = f"{failure.exception_name or 'Failure'}: {failure.detail}"
            inc = await self.ticket.open_incident(
                summary=incident_summary(run), requested_by=run.created_by,
                responsible_group=group,
                flow_type=st.workflow.automation_id if failure and failure.failed_task else None,
                failed_task=failure.failed_task if failure else None, comment=detail,
                description=incident_description(run, detail))
            st.incident_id = inc.ticket_id     # the team we *asked* for; where it landed is the
                                               # adapter's business (it resolves and may fall back)
        if st.ticket is None:       # failed before the ticket existed — nobody to tell
            return
        note = policy.close_comment.format(incident_id=st.incident_id)
        await self.ticket.close_ticket(st.ticket, note=note,
                                       outcome=TicketOutcome.UNSUCCESSFUL)
        st.ticket_closed = True
```

## `orchestration/executor.py` — the run driver

Given a `run_id`, load the run, advance it through as many synchronous steps as possible this
wake-up, and either complete it, schedule it for a later poll/retry (by setting `scheduled_at` —
a worker re-drives it), or mark it `FAILED` and escalate. It only reads and writes the run
store — it knows nothing about how it was invoked.

```python
import logging
import random
import uuid
from datetime import timedelta

from orchestrator.config import Settings
from orchestrator.domain import (RunStatus, StaleRunError, StepDeadlineExceeded, StepName,
                                 WorkflowRun, utcnow)
from orchestrator.orchestration.escalator import FailureEscalator
from orchestrator.orchestration.plans import RUN_PLANS
from orchestrator.orchestration.steps import StepHandler
from orchestrator.ports import WorkflowRunRepository

logger = logging.getLogger(__name__)


class RunExecutor:
    def __init__(self, handlers: dict[StepName, StepHandler], runs: WorkflowRunRepository,
                 settings: Settings, escalator: FailureEscalator) -> None:
        self.handlers = handlers
        self.runs = runs
        self.settings = settings
        self.escalator = escalator

    async def handle(self, run_id: uuid.UUID) -> None:
        run = await self.runs.get(run_id)
        if run is None:
            logger.warning("Run %s not found; dropping message.", run_id)
            return
        if run.status in (RunStatus.COMPLETED, RunStatus.FAILED):
            return                                   # terminal: ignore a duplicate/stale delivery

        plan = RUN_PLANS[run.run_type]
        step = StepName(run.current_step) if run.current_step else plan[0]
        run.status = RunStatus.RUNNING

        while True:                                  # advance through synchronous steps
            run.current_step = step
            handler = self.handlers[step]
            started = run.run_state.step_started_at.setdefault(step, utcnow())
            try:
                if utcnow() - started > timedelta(seconds=handler.max_step_duration_seconds):
                    raise StepDeadlineExceeded(f"Step {step} exceeded its wall-clock budget")
                done = await handler.execute(run)
            except Exception as error:
                await self._on_step_error(run, step, handler, error)
                return

            if not done:                             # async wait → schedule a later poll
                await self._schedule(run, delay=handler.poll_interval_seconds)
                return

            run.run_state.step_attempts.pop(step, None)
            run.run_state.step_started_at.pop(step, None)
            idx = plan.index(step)
            if idx + 1 < len(plan):
                step = plan[idx + 1]
                continue                             # run the next synchronous step now
            run.status, run.current_step, run.scheduled_at = RunStatus.COMPLETED, None, None
            await self._save(run)
            logger.info("Run %s completed.", run.run_id)
            return

    async def _on_step_error(self, run: WorkflowRun, step: StepName,
                             handler: StepHandler, error: Exception) -> None:
        attempts = run.run_state.step_attempts
        attempts[step] = attempts.get(step, 0) + 1
        # A classified failure can be one there is no point retrying (a validation error fails
        # identically on every re-run, and each RUN_ENGINE retry costs a whole fresh engine run).
        # Anything unclassified is a TASK failure: retried, exactly as before.
        kind = error.kind if isinstance(error, StepFailure) else FailureKind.TASK
        retryable = policy_for(kind).retryable
        if retryable and attempts[step] <= run.max_retries:
            handler.reset_for_retry(run.run_state)
            run.run_state.step_started_at.pop(step, None)
            backoff = (self.settings.retry_base_seconds * 2 ** (attempts[step] - 1)
                       + random.uniform(1, 5))
            await self._schedule(run, delay=backoff)
            logger.warning("Run %s step %s failed (attempt %s); retry in %.0fs: %s",
                           run.run_id, step, attempts[step], backoff, error)
            return
        # Exhausted (or never retryable) → terminal FAILED + escalate per the failure policy.
        # Nothing is rolled back.
        run.run_state.errors[step] = str(error)
        run.status, run.scheduled_at = RunStatus.FAILED, None
        await self.escalator.escalate(run, error)
        await self._save(run)
        logger.critical("Run %s step %s exhausted retries: %s — marked FAILED.",
                        run.run_id, step, error)

    async def _schedule(self, run: WorkflowRun, delay: float) -> None:
        run.scheduled_at = utcnow() + timedelta(seconds=delay)   # a worker re-drives when due
        await self._save(run)

    async def _save(self, run: WorkflowRun) -> None:
        try:
            await self.runs.save(run)
        except StaleRunError:
            # Lost to a concurrent writer (an overlapping re-drive) — its save already carries
            # equivalent-or-newer state, and a worker re-drives if anything is left to do.
            logger.info("Run %s save lost to a concurrent writer; a worker will re-drive.", run.run_id)
```

## What got simpler here (vs. the previous revision)

- **`RunStrategy` ABC + `AutomationRunStrategy` + `ResourceRunStrategy` + `build_strategies()`
  are deleted** — four classes that encoded two lists. `RUN_PLANS` is the same information as a
  dict literal, and the handler registry is one function.
- **Finalization is two focused, single-client steps**: `FinalizeResourceStep` (resource runs
  only — marks the resource provisioned) and `CloseTicketStep` (closes the RITM). Each guards on
  its own idempotency marker (`resource_finalized` / `ticket_closed`), so a re-drive resumes at the
  exact step that hadn't finished.
- **No webhook branches** in `RunEngineStep` — the `*_completed`-via-callback checks are gone;
  polling is the single completion path (see [01-external-contracts](01-external-contracts.md)).
- **Typed state everywhere**: `st.ticket`, `st.engine_run_id`, `st.step_attempts[StepName.X]` —
  the `_idem_key`/`_ticket_ref` dict-plumbing helpers are gone (`idem_key` reads typed fields;
  the ticket ref *is* `st.ticket`).
- **`handle_failure()` removed from the handler ABC** — recording the error is one executor line
  (`state.errors[step] = str(error)`); no handler ever overrode it.
