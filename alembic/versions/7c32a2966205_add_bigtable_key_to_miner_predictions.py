"""add bigtable_key to miner_predictions

Revision ID: 7c32a2966205
Revises: 668cbf4585eb
Create Date: 2026-05-25 22:16:27.015614

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "7c32a2966205"
down_revision: Union[str, None] = "668cbf4585eb"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "miner_predictions",
        sa.Column("bigtable_key", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("miner_predictions", "bigtable_key")
