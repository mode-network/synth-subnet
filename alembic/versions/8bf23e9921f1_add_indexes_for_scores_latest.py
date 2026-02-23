"""add indexes for scores latest

Revision ID: 8bf23e9921f1
Revises: 26ab499a7e04
Create Date: 2026-02-16 15:59:06.061937

"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "8bf23e9921f1"
down_revision: Union[str, None] = "26ab499a7e04"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # CREATE INDEX CONCURRENTLY cannot run inside a transaction
    op.execute("COMMIT")

    query = """CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_mp_vr_id
  ON miner_predictions (validator_requests_id);"""

    op.execute(query)

    query = """CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ms_mpid_scored_time_desc
  ON miner_scores (miner_predictions_id, scored_time DESC);"""

    op.execute(query)


def downgrade() -> None:
    # DROP INDEX CONCURRENTLY cannot run inside a transaction
    op.execute("COMMIT")

    query = """DROP INDEX CONCURRENTLY IF EXISTS idx_mp_vr_id;"""
    op.execute(query)

    query = (
        """DROP INDEX CONCURRENTLY IF EXISTS idx_ms_mpid_scored_time_desc;"""
    )
    op.execute(query)
