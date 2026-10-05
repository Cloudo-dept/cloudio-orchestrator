"""add the 'legacy' run type and engine type.

Legacy requests (the legacy automation runner's own wire shape) pass through the orchestrator as a
third run type: one step, which hands the request over, and no ticket/resource/engine work. The
matching engine type labels what will run it — the legacy runner has no WorkflowEngineClient and no
entry in the engines mapping, because it cannot be polled.

StepName.SUBMIT_LEGACY needs no migration: current_step is a plain string column, deliberately, so
the step list can evolve without one.

Revision ID: 0005_legacy_run_type
Revises: 0004_resource_id_index
Create Date: 2026-10-05
"""

from alembic import op

revision = "0005_legacy_run_type"
down_revision = "0004_resource_id_index"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ALTER TYPE ... ADD VALUE cannot run inside a transaction block.
    with op.get_context().autocommit_block():
        op.execute("ALTER TYPE run_type ADD VALUE IF NOT EXISTS 'legacy'")
        op.execute("ALTER TYPE workflow_engine_type ADD VALUE IF NOT EXISTS 'legacy'")


def downgrade() -> None:
    # PostgreSQL cannot drop a value from an enum type; nothing to undo.
    pass
