# Local-dev image for the CloudIO orchestrator — serves the API, runs the worker, and runs
# Alembic migrations (the command is chosen per compose service).
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

# uv: byte-compile on install, copy from cache (bind mounts aren't shared), venv in /app/.venv,
# and put that venv on PATH so `orchestrator` / `uvicorn` / `alembic` resolve directly.
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/app/.venv \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app

# 1) Dependencies only — cached on the lockfile so source edits don't re-resolve.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project

# 2) Source, then install the project itself (editable, so a mounted ./src hot-reloads).
COPY . .
RUN uv sync --frozen

EXPOSE 8000
CMD ["orchestrator", "api"]
