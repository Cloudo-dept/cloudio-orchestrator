"""add the engine-run-id lookup index.

A workflow-engine completion callback resolves the waiting run by its engine run id
(``run_state.engine_run_id``). This partial JSONB index makes ``find_by_engine_run_id`` an index
scan, mirroring the ticket/resource lookup indexes from the initial schema.

Revision ID: 0003_engine_run_id_index
Revises: 0002_rejected_status
Create Date: 2026-07-07
"""

from alembic import op

revision = "0003_engine_run_id_index"
down_revision = "0002_rejected_status"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "CREATE INDEX idx_runs_engine_run_id ON workflow_runs "
        "((run_state #>> '{engine_run_id}')) "
        "WHERE (run_state #>> '{engine_run_id}') IS NOT NULL"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_runs_engine_run_id")
