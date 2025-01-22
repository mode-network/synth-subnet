"""add process_time column in miner_predictions table

Revision ID: a122779998c1
Revises: a361b28ffa14
Create Date: 2025-01-22 15:30:53.105524

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "a122779998c1"
down_revision: Union[str, None] = "a361b28ffa14"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "miner_predictions",
        sa.Column("process_time", sa.Float(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("miner_predictions", "process_time")
