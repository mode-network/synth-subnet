"""add composite index on metagraph_history for lateral join optimization

Revision ID: 187893f603bd
Revises: 8bf23e9921f1
Create Date: 2026-02-23 12:00:00.000000

"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "187893f603bd"
down_revision: Union[str, None] = "8bf23e9921f1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # CREATE INDEX CONCURRENTLY cannot run inside a transaction
    op.execute("COMMIT")
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_metagraph_history_uid_updated_at
        ON metagraph_history (neuron_uid, updated_at DESC)
        INCLUDE (ip_address)
        """)


def downgrade() -> None:
    # DROP INDEX CONCURRENTLY cannot run inside a transaction
    op.execute("COMMIT")
    op.execute("""
        DROP INDEX CONCURRENTLY IF EXISTS ix_metagraph_history_uid_updated_at
        """)
