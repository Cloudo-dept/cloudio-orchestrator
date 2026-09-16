"""add the resource-record lookup index.

A resource's owner asks for the latest run against one resource manager record
(``run_state.resource.resource_id``). This partial JSONB index carries ``created_at`` after the path
expression, so ``find_last_by_resource_id`` stops at the first row instead of sorting every
run that record ever had.

Revision ID: 0004_resource_id_index
Revises: 0003_engine_run_id_index
Create Date: 2026-09-16
"""

from alembic import op

revision = "0004_resource_id_index"
down_revision = "0003_engine_run_id_index"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "CREATE INDEX idx_runs_resource_id ON workflow_runs "
        "((run_state #>> '{resource,resource_id}'), created_at DESC) "
        "WHERE (run_state #>> '{resource,resource_id}') IS NOT NULL"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_runs_resource_id")
