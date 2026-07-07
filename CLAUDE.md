# CloudIO Orchestrator

Orchestrator core for our private cloud: drives workflow runs (ticket → resource → engine →
finalize) over a Postgres-backed run store and a lean queue port. The full design lives in
[docs/](docs/README.md) — read it before implementing anything.

## Engineering rules

1. **Stay generic — never couple to a specific technology's features.** Business logic
   (domain, orchestration, services) depends only on the ports in `ports.py`. Anything that
   mentions pgqueuer, ServiceNow, Airflow, Project Manager, or SQL lives in `adapters/` and
   implements a port. Provider vocabulary (sys_ids, DAG params, queue job types) must never
   cross a port boundary — if a port method only makes sense for one provider, it's wrong.

2. **Use Pydantic everywhere you can.** Explicit models with well-defined schemas beat untyped
   dicts. No `dict[str, Any]` for structured data — define a model (see `RunState`,
   `ResourceSpec`, `EngineFailure`). Free-form pass-through payloads to external systems are the
   only exception, and they must be documented as such.

3. **Type everything.** Full annotations on every function, method, and attribute.
   `uv run mypy` (strict) must pass — no `# type: ignore` without a comment explaining why.

4. **Use design patterns over spaghetti.** Small composable objects (repository, adapter,
   handler-per-step, declarative plans) instead of long functions with heavy branching logic.
   But the simplest pattern that fits: prefer a dict literal over a class hierarchy that encodes
   a list (see `RUN_PLANS`). No speculative abstraction — add a seam when a second
   implementation exists or is genuinely planned.

5. **Dependency injection / IoC always.** Every component receives its dependencies through its
   constructor, typed as ports. `bootstrap.py` is the **only** composition root — nothing else
   constructs an adapter, client, or repository. FastAPI routes get services via `Depends`.

6. **Never name things after Python reserved words or builtins.** No `id`, `type`, `input`,
   `list`, `dict`, etc. as field, attribute, variable, or parameter names — use a domain-specific
   name instead (`run_id`, `run_type`, …). It keeps names unambiguous and avoids shadowing.

## Toolchain

- **uv** manages everything: `uv sync`, `uv run pytest`, `uv run ruff check`, `uv run mypy`.
- **ruff** is the linter *and* formatter; **mypy** runs strict.
- Tests: `pytest` + `pytest-asyncio`; `respx` for httpx clients; `testcontainers[postgres]` for
  repository/queue integration tests.
- Migrations: Alembic (`alembic.ini` at repo root; scripts under
  `src/orchestrator/adapters/migrations`).
