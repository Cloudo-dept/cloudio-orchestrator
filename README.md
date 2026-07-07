# CloudIO Orchestrator

Orchestrator core for our private cloud. It drives workflow runs (**ticket → resource → engine →
finalize**) over a Postgres-backed run store, using a ports-and-adapters design: business logic
depends only on abstract ports, and every technology-specific class (Postgres, ServiceNow, Airflow,
Project Manager) is an adapter bound in one composition root.

There is **no separate queue and no scheduler** — a pool of `RunWorker` loops claims due runs
straight from the run store (`scheduled_at` + `FOR UPDATE SKIP LOCKED`) and drives them; a re-drive
lease provides at-least-once and crash recovery. The full design lives in **[docs/](docs/README.md)**.

- **Stack:** Python 3.12 · [uv](https://docs.astral.sh/uv/) · FastAPI · Pydantic v2 · SQLModel ·
  SQLAlchemy (async) + asyncpg · Alembic · Typer.
- **Gates:** `ruff` (lint + format), `mypy --strict`, `pytest`.

## Requirements

- **[uv](https://docs.astral.sh/uv/)** — manages the toolchain, the virtualenv, and the Python
  interpreter. Install it:
  ```bash
  curl -LsSf https://astral.sh/uv/install.sh | sh
  ```
  uv reads `.python-version` and fetches **Python 3.12** automatically — you do not need it
  installed system-wide.
- **PostgreSQL** — to run the service (and for the integration test suite).
- **Docker** — optional, only for the Postgres integration tests (they auto-skip without it).

## Install

```bash
uv sync            # creates the venv and installs runtime + dev dependencies
```

Run any command inside the environment with `uv run <cmd>` (e.g. `uv run pytest`). The project also
installs a console script named `orchestrator`.

## Configure

Settings come from environment variables (prefix `ORCH_`) or a local `.env` file:

```bash
cp .env.example .env
# edit .env — at minimum set ORCH_DATABASE_URL and the Airflow / ServiceNow / Project Manager creds
```

See [.env.example](.env.example) for every setting and its default.

## Run

**1. Apply database migrations** (creates the enums, tables, and indexes):

```bash
uv run alembic upgrade head
```

**2. Serve the HTTP API** (register workflows, trigger runs, query status):

```bash
uv run orchestrator api                 # defaults to 0.0.0.0:8000
uv run orchestrator api --host 127.0.0.1 --port 9000
```

Interactive API docs are then at `http://localhost:8000/docs`. Two probes support monitoring:
`GET /healthz` (liveness — the process is serving, no DB touch) and `GET /readyz` (readiness —
`200` when the run store is reachable, `503` when it is not). The `api` compose service uses
`/healthz` as its healthcheck, so `docker compose ps` reports `(healthy)` and the `ui` waits for it.

**3. Run the worker daemon** (claims due runs and drives them forward):

```bash
uv run orchestrator worker
```

The API and the worker are separate processes over the same database. Run one or more workers —
`SKIP LOCKED` keeps their claims disjoint, so scaling is just more worker processes.

Quick smoke check of the CLI:

```bash
uv run orchestrator --help
```

## Run with Docker (local dev)

A `docker-compose.yml` brings up Postgres, applies migrations, and starts the API + worker —
no local Python needed.

```bash
cp .env.example .env      # provider creds; placeholder values are fine to boot locally
docker compose up --build
```

This starts:

- **postgres** — Postgres 16 for the orchestrator on `localhost:5432` (`pgdata` volume).
- **migrate** — runs `alembic upgrade head` once, then exits.
- **api** — the FastAPI app on `http://localhost:8000` (docs at `/docs`), with `--reload`.
- **worker** — the claim-and-drive daemon.
- **airflow** — Airflow 3.0.3 standalone (the workflow engine) on `http://localhost:8080`. Drop
  DAGs in `./airflow/dags`. Login is a fixed **`admin` / `password`** (compose seeds
  SimpleAuthManager's passwords file so it never generates a random one) — set
  `ORCH_AIRFLOW_PASSWORD=password` in `.env`. First boot still takes ~a minute to initialize.
- **project-manager** + **pm-postgres** — a dev-only Project Manager API mock (FastAPI, in
  `dev/project_manager/`) with its **own** Postgres, on `http://localhost:8081`. Beyond the
  resource endpoints the orchestrator calls, it exposes `POST /projects` (seed a project) and
  `GET /projects` for local convenience.
- **ui** — a dev browser console (CRUD + live run monitor) on `http://localhost:8090`. It's a
  standalone stdlib server (`dev/ui/`) that serves the static GUI and reverse-proxies `/api` to the
  orchestrator, so the API stays untouched. Run it outside compose with
  `python dev/ui/server.py` (set `ORCH_API_BASE` if the API isn't on `localhost:8000`).

`ORCH_DATABASE_URL`, `ORCH_AIRFLOW_BASE_URL`, and `ORCH_PM_BASE_URL` are set by compose to point at
those services, overriding `.env` (only the Airflow/ServiceNow/PM *credentials* come from `.env`).
`./src` is mounted into the containers (the project is installed editable), so code edits hot-reload
the API. Useful variations:

```bash
docker compose up postgres          # just the database (then run the app locally with uv)
docker compose up --scale worker=3  # several worker processes (SKIP LOCKED keeps claims disjoint)
docker compose run --rm api pytest -m "not integration"   # run the suite inside the image
docker compose down -v              # stop everything and drop the database volume
```

## Test

The suite is split into fast **unit** tests (pure, no I/O — domain, orchestration, services, the
worker loop) and **integration** tests (HTTP adapters against in-process mock servers, plus
Postgres-backed repository / end-to-end tests).

```bash
uv run pytest                    # everything; Postgres tests skip automatically without Docker
uv run pytest tests/unit         # only the fast, DB-free tests
uv run pytest -m "not integration"   # skip the Docker-gated Postgres tests explicitly
uv run pytest -v                 # verbose, per-test
```

- **No Docker?** The tests that need a real Postgres (via `testcontainers`) are marked `integration`
  and **skip cleanly** — the rest still run and pass.
- **With Docker running**, the full suite executes, spinning up a throwaway Postgres for the
  repository, worker-claim, and end-to-end tests.

The provider integration tests drive the **real** ServiceNow / Airflow / Project Manager adapters
against stateful in-process mock servers (no network), so request-building, auth refresh, and
idempotency are exercised for real.

## Quality gates

```bash
uv run ruff check                # lint (and import sorting)
uv run ruff format               # format
uv run mypy                      # strict type-check (src/)
```

All three must be clean; `mypy` runs in strict mode.

## Project layout

```text
src/orchestrator/
├── config.py          # Settings (pydantic-settings)
├── domain.py          # enums · value objects · RunState · WorkflowRun/Workflow (SQLModel)
├── ports.py           # the five ABCs (two repositories, three clients)
├── services.py        # WorkflowService · WorkflowRunService
├── orchestration/     # step handlers · run plans · RunExecutor · FailureEscalator
├── adapters/          # Postgres repos · ServiceNow · Airflow · Project Manager · migrations
├── api.py             # FastAPI app + HTTP schemas
├── worker.py          # OrchestratorWorker (daemon) + RunWorker (claim-and-drive loop)
├── cli.py             # `orchestrator api` / `orchestrator worker`
└── bootstrap.py       # composition root — the only place adapters are bound to ports
tests/
├── unit/              # domain, orchestration, services, worker loop — over in-memory fakes
├── integration/       # HTTP adapters vs mock servers · Postgres repo · API · end-to-end
├── mocks/             # one FastAPI mock server per provider
├── fakes.py           # in-memory implementations of the five ports
└── factories.py       # entity builders
```

## Documentation

The design docs (architecture, domain, orchestration, external contracts, testing strategy) are in
**[docs/](docs/README.md)** — start with the index. Engineering rules are in
[CLAUDE.md](CLAUDE.md).
