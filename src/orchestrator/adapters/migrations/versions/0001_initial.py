"""initial schema: enums, workflow_runs, workflows, and the claim/lookup indexes.

Revision ID: 0001_initial
Revises:
Create Date: 2026-07-06
"""

from alembic import op
from sqlmodel import SQLModel

import orchestrator.domain  # noqa: F401  (populates SQLModel.metadata)

revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()

    # Native PG enum types (the SQLModel columns declare create_type=False, so we own them here).
    op.execute("CREATE TYPE run_type AS ENUM ('automation', 'resource')")
    op.execute("CREATE TYPE run_status AS ENUM ('pending','running','completed','failed')")
    op.execute("CREATE TYPE workflow_engine_type AS ENUM ('airflow')")

    # workflow_runs + workflows (and their column-level indexes) from the SQLModel metadata.
    SQLModel.metadata.create_all(bind)

    # Claim index: workers claim due runs by scheduled_at; partial predicate keeps terminal/
    # in-flight (NULL) rows out of the index.
    op.execute(
        "CREATE INDEX idx_runs_scheduled_at ON workflow_runs (scheduled_at) "
        "WHERE scheduled_at IS NOT NULL"
    )
    # Lookup indexes for list-by-ticket / list-by-resource (typed JSONB paths).
    op.execute(
        "CREATE INDEX idx_runs_ticket_id ON workflow_runs ((run_state #>> '{ticket,ticket_id}')) "
        "WHERE (run_state #>> '{ticket,ticket_id}') IS NOT NULL"
    )
    op.execute(
        "CREATE INDEX idx_runs_resource_vendor_id ON workflow_runs "
        "((run_state #>> '{resource,vendor_id}')) "
        "WHERE (run_state #>> '{resource,vendor_id}') IS NOT NULL"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_runs_resource_vendor_id")
    op.execute("DROP INDEX IF EXISTS idx_runs_ticket_id")
    op.execute("DROP INDEX IF EXISTS idx_runs_scheduled_at")
    op.drop_table("workflow_runs")
    op.drop_table("workflows")
    op.execute("DROP TYPE IF EXISTS workflow_engine_type")
    op.execute("DROP TYPE IF EXISTS run_status")
    op.execute("DROP TYPE IF EXISTS run_type")
