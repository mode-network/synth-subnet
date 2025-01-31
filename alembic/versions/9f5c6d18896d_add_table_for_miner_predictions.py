"""add table for miner predictions

Revision ID: 9f5c6d18896d
Revises:
Create Date: 2024-12-07 20:10:49.131459

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSON


# revision identifiers, used by Alembic.
revision: str = "9f5c6d18896d"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "miner_predictions",
        sa.Column("id", sa.BigInteger, primary_key=True),
        sa.Column("miner_uid", sa.Integer, nullable=False),
        sa.Column("start_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("prediction", JSON, nullable=False),
    )
    op.create_index("ix_start_time", "miner_predictions", ["start_time"])


def downgrade() -> None:
    op.drop_index("ix_start_time", table_name="miner_predictions")
    op.drop_table("miner_predictions")
