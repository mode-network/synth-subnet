"""add scored_time to miner_scores table

Revision ID: 6778da854170
Revises: a9177927599a
Create Date: 2025-01-03 19:31:06.787524

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "6778da854170"
down_revision: Union[str, None] = "a9177927599a"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "miner_scores",
        sa.Column("scored_time", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_miner_scores_scored_time", "miner_scores", ["scored_time"]
    )


def downgrade() -> None:
    op.drop_index("ix_miner_scores_scored_time", table_name="miner_scores")
    op.drop_column("miner_scores", "scored_time")
