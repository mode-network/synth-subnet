"""add score v4

Revision ID: 1b8a8038d5eb
Revises: 26ab499a7e04
Create Date: 2026-02-09 18:10:27.571647

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

# revision identifiers, used by Alembic.
revision: str = "1b8a8038d5eb"
down_revision: Union[str, None] = "26ab499a7e04"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "miner_scores", sa.Column("prompt_score_v4", sa.Float, nullable=True)
    )
    op.add_column(
        "miner_scores", sa.Column("score_details_v4", JSONB, nullable=True)
    )


def downgrade() -> None:
    op.drop_column("miner_scores", "prompt_score_v4")
    op.drop_column("miner_scores", "score_details_v4")
