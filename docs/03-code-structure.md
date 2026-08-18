*[← Index](README.md)*

# Code Architecture & Directory Structure

The layout keeps exactly one architectural idea: **ports vs. adapters**. Business logic (domain,
orchestration, services) depends only on abstract ports; every technology-specific class
(Postgres, ServiceNow, Airflow, Project Manager) lives in `adapters/` and implements a
port. `bootstrap.py` is the **only** place adapters are bound to ports (composition root / IoC).

Everything else is deliberately flat: modules are grouped **by role**, not one-class-per-file.
~16 modules instead of ~50 — same seams, half the ceremony.

| Module group | Holds | Depends on |
|---|---|---|
| `domain.py` | enums, typed entities (`WorkflowRun`, `Workflow`, `RunState`, …), exceptions, `utcnow` | pydantic, sqlmodel |
| `ports.py` | the five ABCs every adapter implements (the two repositories, the three clients) | `domain` |
| `orchestration/` | step handlers, run plans, `RunExecutor`, `FailureEscalator` | `domain`, `ports` |
| `services.py` | `WorkflowService`, `WorkflowRunService` (use-cases the API delegates to) | `domain`, `ports` |
| `adapters/` | concrete port implementations + Alembic migrations | `domain`, `ports`, tech libs |
| `api.py` / `worker.py` / `cli.py` / `bootstrap.py` | delivery + wiring | everything |

**Dependency rule:** `orchestration` and `services` import only `domain` and `ports` — they never
see ServiceNow/Airflow/SQL vocabulary. Swapping ServiceNow for another ITSM, or Airflow for
another engine, means writing one new adapter module and changing one line in `bootstrap.py`.

### Directory tree

```text
cloudio-orchestrator/
├── pyproject.toml                      # uv-managed; ruff + mypy + pytest config
├── uv.lock
├── alembic.ini                         # script_location → src/orchestrator/adapters/migrations
├── .env.example
├── CLAUDE.md                           # project engineering rules
├── README.md
├── docs/                               # this design (README + 01–11)
├── tests/                              # layout + mock-server harness detailed in docs/11-testing.md
│   ├── conftest.py                     # mock instances, mock-wired real clients, pg_session_factory
│   ├── fakes.py                        # in-memory port impls (clients, repos) for unit tests
│   ├── mocks/                          # one FastAPI mock app per provider (faithful + Override knob)
│   │   ├── base.py                     # Override/apply_overrides + MockServer (ephemeral-port option)
│   │   ├── servicenow.py               # ServiceNowMock
│   │   ├── airflow.py                  # AirflowMock
│   │   └── project_manager.py          # ProjectManagerMock
│   ├── unit/                           # port-level: domain, orchestration, worker, services (fakes)
│   │   ├── test_domain.py
│   │   ├── test_orchestration.py       # executor, steps, retry/failure paths
│   │   ├── test_worker.py              # RunWorker loop: claims 1 → drives → empty-poll backoff
│   │   └── test_services.py
│   └── integration/                    # wire-level: real adapters + real Postgres
│       ├── test_adapter_airflow.py     # real client vs AirflowMock
│       ├── test_adapter_servicenow.py
│       ├── test_adapter_project_manager.py
│       ├── test_repository.py          # testcontainers Postgres: concurrency, claim_due, JSONB finders
│       ├── test_worker_claim.py        # testcontainers: claim_due + drive, SKIP LOCKED disjoint, lease re-drive
│       ├── test_api.py
│       └── test_end_to_end.py          # full run over the 3 mocks + real Postgres
├── config/
│   └── log-config.example.json     # dictConfig: ecs_logging.StdlibFormatter → stdout
└── src/
    └── orchestrator/
        ├── __init__.py
        ├── config.py                   # Settings (pydantic-settings) — single database_url
        ├── log.py                      # configure_logging + LOG_CONFIG_PATH + request log context
        ├── domain.py                   # enums · TicketRef/ResourceSpec/RunState · WorkflowRun/Workflow (SQLModel) · exceptions
        ├── ports.py                    # WorkflowRunRepository · WorkflowRepository ·
        │                               # TicketSystemClient · ResourceManagerClient · WorkflowEngineClient
        ├── services.py                 # WorkflowService · WorkflowRunService
        ├── orchestration/
        │   ├── __init__.py
        │   ├── steps.py                # StepHandler ABC + CreateTicketStep/ConfigureResourceStep/RunEngineStep/FinalizeResourceStep/CloseTicketStep
        │   ├── plans.py                # RUN_PLANS (RunType → ordered StepNames) + build_handlers()
        │   ├── executor.py             # RunExecutor (drives one run per call)
        │   └── escalator.py            # FailureEscalator
        ├── adapters/
        │   ├── __init__.py
        │   ├── database.py             # async engine/session factory + PostgresWorkflowRunRepository + PostgresWorkflowRepository
        │   ├── servicenow.py           # ServiceNowTicketClient → implements TicketSystemClient
        │   ├── airflow.py              # AirflowWorkflowEngineClient → implements WorkflowEngineClient
        │   ├── project_manager.py      # ProjectManagerResourceClient → implements ResourceManagerClient
        │   └── migrations/             # Alembic env.py + versions/0001_initial.py
        ├── api.py                      # FastAPI app + request/response schemas + routers
        ├── worker.py                   # OrchestratorWorker (daemon) + RunWorker (claim-and-drive loop)
        ├── cli.py                      # typer app: `orchestrator api` / `orchestrator worker`
        └── bootstrap.py                # build(settings) → Container (the composition root)
```

Why `src/orchestrator/` and not just `src/`: `orchestrator` is the importable package name
(`from orchestrator.ports import WorkflowRunRepository`); `src/` is only the src-layout wrapper that keeps the
package out of the repo root so tests import the *installed* copy.

### Component → module mapping

| Component (docs 04–08) | Module |
|---|---|
| `Settings` | `config.py` |
| enums, `TicketRef`, `ResourceSpec`, `ResolvedWorkflow`, `EngineFailure`, `RunState`, `WorkflowRun`, `Workflow`, exceptions, `utcnow`, `PydanticJSONB` | `domain.py` |
| all five port ABCs | `ports.py` |
| session factory, `PostgresWorkflowRunRepository`, `PostgresWorkflowRepository` | `adapters/database.py` |
| `ServiceNowTicketClient` / `AirflowWorkflowEngineClient` / `ProjectManagerResourceClient` | `adapters/servicenow.py` / `airflow.py` / `project_manager.py` |
| `StepHandler` + the four step handlers | `orchestration/steps.py` |
| `RUN_PLANS`, `build_handlers` | `orchestration/plans.py` |
| `RunExecutor` | `orchestration/executor.py` |
| `FailureEscalator` | `orchestration/escalator.py` |
| `WorkflowService`, `WorkflowRunService` | `services.py` |
| FastAPI app, HTTP schemas, routers | `api.py` |
| `OrchestratorWorker`, `RunWorker` | `worker.py` |
| typer CLI | `cli.py` |
| `configure_logging`, `RequestContextFilter`, `RequestContextMiddleware` | `log.py` |
| `build()` / `Container` | `bootstrap.py` |

### Toolchain (uv + modern stack)

The project is managed with **[uv](https://docs.astral.sh/uv/)** — `uv sync` creates the env,
`uv run <cmd>` runs inside it, `uv lock` pins.

```toml
# pyproject.toml (essentials)
[project]
name = "cloudio-orchestrator"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    "fastapi",
    "uvicorn[standard]",
    "pydantic>=2",
    "pydantic-settings",
    "sqlmodel",
    "sqlalchemy[asyncio]",
    "asyncpg",
    "httpx",
    "alembic",
    "typer",
]

[dependency-groups]
dev = [
    "pytest",
    "pytest-asyncio",
    "respx",                    # declarative httpx mocking for the client tests
    "testcontainers[postgres]", # real Postgres for repository/queue integration tests
    "ruff",
    "mypy",
]

[project.scripts]
orchestrator = "orchestrator.cli:app"

[tool.ruff]
line-length = 100

[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B", "ASYNC"]

[tool.mypy]
strict = true

[tool.pytest.ini_options]
asyncio_mode = "auto"
```

- **ruff** is both linter and formatter (replaces black/isort/flake8).
- **mypy `strict`** enforces the "typing everywhere" rule.
- **respx** mocks httpx per-route — cleaner client tests than hand-rolled `MockTransport`.
- **testcontainers** spins a throwaway Postgres so `claim_due`/`SKIP LOCKED`/JSONB tests run
  against the real thing.
- **typer** gives the two entrypoints (`orchestrator api`, `orchestrator worker`) as one installed
  script.

### Design notes

- **Ports in one module.** All six ABCs are small (2–5 methods); a single `ports.py` shows the
  entire external surface of the system on one page. Adapters grow independently.
- **One model, not three.** `WorkflowRun` is a single SQLModel class — it *is* a Pydantic model
  and *is* the table. The previous revision's pure-entity + `WorkflowRunTable` + mapper trio is
  gone; that's what SQLModel exists to avoid. The repository port still keeps SQL out of the
  business logic.
- **Typed state, no magic dict.** The run's working state is `RunState`, an explicit Pydantic
  model persisted as one JSONB column via a 10-line `PydanticJSONB` type decorator (validate on
  load, `model_dump` on save). No `MutableDict`, no stringly-typed keys.
- **Plans are data, not classes.** The step order per run type is a literal
  `dict[RunType, tuple[StepName, ...]]`; handlers are a `dict[StepName, StepHandler]` built once
  in bootstrap. The `RunStrategy` ABC + two subclasses + factory from the previous revision are
  deleted — they encoded a list.
- **No queue module and no scheduler** — each `RunWorker` claims one due run (`claim_due(1)`,
  `SKIP LOCKED`) and drives it, then loops; the run store's `scheduled_at` + `SKIP LOCKED` + a
  re-drive lease already are the durable work queue, so there is nothing to schedule or transport.
