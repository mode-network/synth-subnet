"""add prompt score v3

Revision ID: 6ebc15f2e397
Revises: 70f4a9879daf
Create Date: 2025-04-29 20:58:59.103299

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB


# revision identifiers, used by Alembic.
revision: str = "6ebc15f2e397"
down_revision: Union[str, None] = "70f4a9879daf"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "miner_scores", sa.Column("prompt_score_v3", sa.Float, nullable=True)
    )
    op.add_column(
        "miner_scores", sa.Column("score_details_v3", JSONB, nullable=True)
    )
    op.alter_column("miner_scores", "score_details", nullable=True)


def downgrade() -> None:
    op.drop_column("miner_scores", "prompt_score_v3")
    op.drop_column("miner_scores", "score_details_v3")
