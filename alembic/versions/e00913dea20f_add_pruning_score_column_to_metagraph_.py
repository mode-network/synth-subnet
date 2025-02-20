"""add pruning score column to metagraph_history table

Revision ID: e00913dea20f
Revises: d0a95572a74c
Create Date: 2025-02-20 13:44:13.481144

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "e00913dea20f"
down_revision: Union[str, None] = "d0a95572a74c"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "metagraph_history",
        sa.Column("pruning_score", sa.Float, nullable=True),
    )


def downgrade() -> None:
    op.drop_column("metagraph_history", "pruning_score")
