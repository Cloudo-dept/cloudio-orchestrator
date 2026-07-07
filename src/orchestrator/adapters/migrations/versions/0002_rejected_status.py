"""add the 'rejected' terminal run status.

A resource run now waits for the RITM to be approved (AWAIT_APPROVAL step); a rejection ends the
run in the new terminal status 'rejected'. AWAIT_APPROVAL itself needs no migration — current_step
is a plain string column.

Revision ID: 0002_rejected_status
Revises: 0001_initial
Create Date: 2026-07-07
"""

from alembic import op

revision = "0002_rejected_status"
down_revision = "0001_initial"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ALTER TYPE ... ADD VALUE cannot run inside a transaction block.
    with op.get_context().autocommit_block():
        op.execute("ALTER TYPE run_status ADD VALUE IF NOT EXISTS 'rejected'")


def downgrade() -> None:
    # PostgreSQL cannot drop a value from an enum type; nothing to undo.
    pass
