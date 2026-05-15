"""add indexes for prune_redundant_predictions

Revision ID: 7d7e8cbdbddd
Revises: 187893f603bd
Create Date: 2026-05-15 09:07:45.872774

"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "7d7e8cbdbddd"
down_revision: Union[str, None] = "187893f603bd"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # CREATE INDEX CONCURRENTLY cannot run inside a transaction.
    op.execute("COMMIT")

    # Serves the validator_requests CTE filter:
    #   WHERE time_length = :tl AND start_time < :cutoff
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS
            ix_vr_time_length_start_time
        ON validator_requests (time_length, start_time)
        """)

    # Serves the final UPDATE on miner_predictions:
    #   WHERE validator_requests_id IN (<small set>) AND deleted_at IS NULL
    # Partial index keeps it small and avoids scanning soft-deleted rows.
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS
            ix_mp_validator_requests_id_alive
        ON miner_predictions (validator_requests_id)
        WHERE deleted_at IS NULL
        """)


def downgrade() -> None:
    # DROP INDEX CONCURRENTLY cannot run inside a transaction.
    op.execute("COMMIT")
    op.execute("""
        DROP INDEX CONCURRENTLY IF EXISTS
            ix_mp_validator_requests_id_alive
        """)
    op.execute("""
        DROP INDEX CONCURRENTLY IF EXISTS
            ix_vr_time_length_start_time
        """)
