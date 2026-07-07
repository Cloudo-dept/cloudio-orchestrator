*[← Index](README.md)*

# Design safeguards

The correctness properties the design preserves, expressed through the simplified stack:

- **Run state is the single source of truth** — durable run state lives in `WorkflowRun`; a `RunWorker` advances a run by claiming its row (`claim_due(1)`, `SKIP LOCKED`) and driving it, with no separate queue store or message to keep in sync. The run store *is* the work queue.
- **Enum persistence** — native PG enums via `values_callable` + `name=` + `create_type=False` on the SQLModel columns; enum *values* (lowercase) match the PG labels.
- **Typed state round-trip** — the run's working state is a `RunState` Pydantic model persisted through `PydanticJSONB` (`model_dump` on save, `model_validate` on load); a schema drift fails loudly at load, not silently at key access. Whole-model writes on `save()` — no in-place mutation tracking, no `MutableDict`.
- **No detached-instance crashes** — `expire_on_commit=False`, `expunge()`-ed read copies out of the repositories.
- **Per-step retry accounting** — `state.step_attempts` (keyed by `StepName`), incremented only on real failure; per-step exponential backoff applied by setting `WorkflowRun.scheduled_at = now + backoff`, which a worker claims and drives when due.
- **No lost updates** — `version` optimistic concurrency on `WorkflowRun` (`StaleRunError`); on conflict the executor drops the lost save and another worker re-drives — it never clobbers. The state-writer path is single in normal operation (the conflict case is an overlapping re-drive after a lease expiry). The wake-early callback is *not* a state writer: `wake(run_id)` only sets `scheduled_at=now` and stays outside the `version` scheme, so it never conflicts with a drive — at worst it triggers one extra harmless re-poll.
- **Exactly-once side effects** — server-side idempotency keys `(run_id, step, attempt)` (Airflow: `dag_run_id`; ServiceNow: `correlation_id`; Project Manager: `Idempotency-Key`), independent of any transport, plus typed idempotency markers in `RunState` (`ticket`, `resource_configured`, `ticket_closed`, …) so a re-driven step skips completed work.
- **Crash recovery** — the re-drive lease: `claim_due` pushes each claimed run's `scheduled_at` forward before driving, so a run whose worker dies becomes due again and is re-driven by another worker. No queue-side heartbeat required — there is no queue.
- **Terminal failure, no rollback** — a step that exhausts retries ends the run at `FAILED` and escalates (Incident + RITM note); partially-created resources are left `in_progress=False` for operator follow-up.
- **Timezone-aware UTC** everywhere; **bounded/validated** requests (typed `ResourceSpec` at the HTTP boundary); **per-step wall-clock deadline** (`StepDeadlineExceeded`).
