"""Use v7 for run and cron defaults and take assistant IDs from their writer."""

import sqlalchemy as sa

from alembic import op

revision = "701ac8735bc1"
down_revision = "a3f7c1d9e2b4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '3s'")
    op.alter_column("assistant", "assistant_id", server_default=None)
    op.alter_column("runs", "run_id", server_default=sa.text("uuidv7()::text"))
    op.alter_column("crons", "cron_id", server_default=sa.text("uuidv7()::text"))


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '3s'")
    previous_default = sa.text("gen_random_uuid()::text")
    op.alter_column("assistant", "assistant_id", server_default=previous_default)
    op.alter_column("runs", "run_id", server_default=previous_default)
    op.alter_column("crons", "cron_id", server_default=previous_default)
