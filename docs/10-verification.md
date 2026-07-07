*[← Index](README.md)*

# Verification Plan

This page is the **what** — the properties every layer must prove. The **how** — the unit/integration
split, in-memory port fakes, and the per-provider mock servers that drive the real adapters — lives in
[11-testing](11-testing.md).

**Automated (`pytest` + `pytest-asyncio`; `respx` for the httpx clients; `testcontainers[postgres]` for real-Postgres repository/worker tests):**

- **Domain / persistence**: enum round-trip; `RunState` round-trips through `PydanticJSONB` (save → load → identical model); a `RunState` with unknown/invalid fields fails validation loudly; per-step retry budget; optimistic-concurrency conflict (`StaleRunError`) → executor drops the lost save, another worker re-drives (never clobbers); idempotent double-execution of every step (typed markers skip completed work); terminal failure path (retries exhausted → run `FAILED`, Incident opened, RITM work note, resource left `in_progress=False`, **nothing rolled back**).
- **Decoupling / driver**: the `RunExecutor` is invoked with only a `run_id` and loads all state from the run store; a `run_id` for a terminal run is a no-op; a run advances through multiple synchronous steps in one wake-up and, at an async wait/retry, sets `scheduled_at` (deferral is run state, never a transport delay).
- **Workers**: `claim_due` returns runs with `scheduled_at <= now`, skips future/NULL ones, and pushes claimed rows' `scheduled_at` forward by the lease (so a crashed drive re-drives); each `RunWorker` claims one run at a time (`claim_due(1)`) and drives it, backing off a poll interval when nothing is due; concurrent workers (in one daemon or many) never claim the same run twice (`SKIP LOCKED`).
- **Airflow client** (v2, token auth, `verify=False`): authenticates via `/auth/token` and refreshes on 401; `trigger_workflow` posts `dag_run_id`/`conf` (409 → idempotent, 404 → "DAG not found"); `query_run_status` maps `queued/running` → `EngineRunStatus.IN_PROGRESS`; `get_failure` returns a typed `EngineFailure` (failed task + responsible group + message) and an empty `EngineFailure` when no failed task is listed; `get_output` probes the run's task instances for a named XCom and returns its value (or `None` when absent).
- **ServiceNow client**: `open_ticket` orders the catalog item, resolves the RITM from the created Request, tags `correlation_id`, returns RITM `number`+`sys_id` (a same-key retry returns the existing RITM); `close_ticket`/`annotate_ticket` PATCH the RITM (state 3 / work note); `open_incident` builds the documented INC payload and resolves the responsible group via the dict (fallback to the exact name). No ServiceNow vocabulary crosses the port.
- **Project Manager client**: `create_resource` POSTs to `/projects/{id}/project_resources/{type}` with the `Idempotency-Key` header; `update_resource` PATCHes by `vendor_id`; finalize PATCHes `in_progress=False` (no delete endpoint).
- **Engine-reported vendor id**: on engine success `RunEngineStep` reads the `final_vendor_id` output and, if present, records it under `state.resource.data.vendor_id`; `FinalizeResourceStep` then PATCHes that vendor id, falling back to the caller-supplied `vendor_id` when the run produced no output (reading the output is best-effort — a fetch error never fails a succeeded run).
- **Failure model**: a permanently-failed step opens an Incident to the responsible group (engine failure → group from `get_failure`; otherwise `cloudio`) and adds a work note to the RITM; escalation errors never crash the worker.
- **Workflow registry / trigger**: register a workflow, then trigger by `workflow_identifier` → `WorkflowRun` built with the mapped `ResolvedWorkflow` snapshot and `scheduled_at=now`; unknown identifier → 404 (`UnknownWorkflowError`); resource workflow without `resource` → 422 (`ResourceParamsRequired`); malformed `ResourceSpec` → 422 from Pydantic at the boundary; both flows begin with `creating_ticket`.
- **Static gates**: `uv run ruff check` clean; `uv run mypy` (strict) clean — the typing rules are enforced, not aspirational.

**Integration / manual:**
- Start ≥2 daemons (each claims + drives); confirm no `run_id` is processed by two workers concurrently and no due run is driven twice (`SKIP LOCKED` + the `version` check).
- Trigger a real Airflow DAG end-to-end; kill a worker mid-drive and confirm another worker re-drives the run once its `scheduled_at` lease expires, resuming from the correct step (idempotent replay via the typed state markers).
- Exercise the retention/purge job (terminal `workflow_runs` rows).
