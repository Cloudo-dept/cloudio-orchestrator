# Orchestrator Core Component Design and Implementation Plan

Architecture, persistence, run scheduling, step state machine, and Python interfaces for the orchestrator core of our private cloud system.

> [!NOTE]
> **Stack:** Python 3.12 · **uv** (project manager) · **Pydantic v2** everywhere (typed models over dicts) · **SQLModel** (one class = entity *and* table) · **pydantic-settings** · **FastAPI** · a pool of **`RunWorker`** loops that each claim one due run (`scheduled_at` + `SKIP LOCKED`) and drive it — no scheduler, no separate queue · a pluggable **workflow-engine** port with **Airflow** first · **ServiceNow** as the first ticket system · **ruff + mypy(strict)** as gates.
>
> **Design principles this revision enforces:**
> 1. **Simple over clever.** ~16 modules instead of ~50; run plans are a dict literal, not a Strategy class hierarchy; one `WorkflowRun` model, not entity+table+mappers; the optional webhook path is cut entirely (polling was already authoritative); the separate queue *and* the scheduler are cut too (workers claim straight from the run store — `scheduled_at` + `SKIP LOCKED` + lease already are a durable queue); unused port methods are deleted.
> 2. **Generic at every seam.** Business logic depends only on five small ports (two repositories, three clients); every technology-specific class lives in `adapters/` and is bound in `bootstrap.py` alone. Swapping Airflow→another engine or ServiceNow→another ITSM is one new adapter + one wiring line.
> 3. **Typed end to end.** The run's working state is an explicit `RunState` Pydantic model persisted as JSONB (no magic-key dicts); engine results and failures are typed (`EngineRunStatus`, `EngineFailure`); mypy strict must pass.
>
> **Two structural decisions:**
> 1. **One durable store, no separate queue.** The durable business object is a **`WorkflowRun`** row; all scheduling (poll interval, retry backoff) is application state (`WorkflowRun.scheduled_at`). A pool of identical `RunWorker` loops each claim one due row (`FOR UPDATE SKIP LOCKED`) and drive it through the executor. `claim_due` + a re-drive lease + optimistic `version` + idempotency keys already give at-least-once and crash recovery, so a dedicated queue (pgqueuer, RabbitMQ) — or even a separate scheduler feeding the workers — would only restate the workers' own claim over the same database. Cut as duplication.
> 2. **No rollback/compensation.** On a step's permanent failure the run terminates as `FAILED` and the failure is escalated (ServiceNow Incident + RITM work note). Partially-created resources are left in place with `in_progress=False` for operator follow-up (there is no delete endpoint anyway).
>
> See [Design safeguards](09-design-safeguards.md) for the retained correctness properties. Project engineering rules live in [CLAUDE.md](../CLAUDE.md).

## Documents

Read in order, or jump to what you need:

| # | Document | Contents |
|---|---|---|
| — | **[Overview](README.md)** (this page) | Stack, design principles, and the two structural decisions. |
| 01 | [External contracts](01-external-contracts.md) | Assumptions on ServiceNow / Airflow / Project Manager that need confirming: idempotency keys, polling completion, failure model. |
| 02 | [Architecture](02-architecture.md) | Workflow registry, the two flows, the single run-state store, the workers-claim-directly coordination model, and the system diagram. |
| 03 | [Code structure](03-code-structure.md) | Ports-and-adapters layout (~16 modules), directory tree, component→module map, and the uv/ruff/mypy toolchain. |
| 04 | [Domain & config](04-domain-and-config.md) | `Settings`, enums, the typed `RunState`, the `WorkflowRun`/`Workflow` SQLModels, and the DDL. |
| 05 | [Stores](05-stores.md) | The two Postgres repositories — the run store and the workflow registry. |
| 06 | [External clients](06-external-clients.md) | The ticket / resource / engine ports and their ServiceNow, Airflow, and Project Manager adapters. |
| 07 | [Orchestration](07-orchestration.md) | Run plans (data, not classes), the four step handlers, the `RunExecutor`, and the `FailureEscalator` — no compensation. |
| 08 | [Entrypoints](08-entrypoints.md) | Services, the FastAPI app, the worker daemon (`OrchestratorWorker` + `RunWorker`), the typer CLI, and the composition root. |
| 09 | [Design safeguards](09-design-safeguards.md) | The correctness properties the design preserves. |
| 10 | [Verification](10-verification.md) | The automated and integration/manual test plan — **what** must hold. |
| 11 | [Testing strategy & harness](11-testing.md) | **How** the suite is built: unit vs. integration, in-memory port fakes, and an easy-to-change mock server per external API (ServiceNow / Airflow / Project Manager). |
