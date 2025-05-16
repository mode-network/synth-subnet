"""add prompt_score_v2

Revision ID: 4cecf5cce9ca
Revises: 3ae76bddd86f
Create Date: 2025-03-06 18:10:45.313545

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB


# revision identifiers, used by Alembic.
revision: str = "4cecf5cce9ca"
down_revision: Union[str, None] = "3ae76bddd86f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "miner_scores", sa.Column("prompt_score_v2", sa.Float, nullable=True)
    )
    op.add_column(
        "miner_scores", sa.Column("score_details_v2", JSONB, nullable=True)
    )
    op.alter_column("miner_scores", "score_details", nullable=True)


def downgrade() -> None:
    op.drop_column("miner_scores", "prompt_score_v2")
    op.drop_column("miner_scores", "score_details_v2")
