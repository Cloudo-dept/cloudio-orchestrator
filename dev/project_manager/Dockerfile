# Dev-only Project Manager mock — tiny FastAPI + asyncpg service.
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

ENV UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/app/.venv \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app
RUN uv venv && uv pip install fastapi "uvicorn[standard]" "sqlalchemy[asyncio]" asyncpg

COPY app.py ./
EXPOSE 8000
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
