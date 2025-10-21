"""add columns reward and real_prices to miner_rewards table

Revision ID: 448fada07788
Revises: 1154ae96bd0a
Create Date: 2024-12-08 16:21:37.857982

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSON


# revision identifiers, used by Alembic.
revision: str = "448fada07788"
down_revision: Union[str, None] = "1154ae96bd0a"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "miner_rewards", sa.Column("reward", sa.Float, nullable=True)
    )
    op.add_column(
        "miner_rewards", sa.Column("real_prices", JSON, nullable=True)
    )


def downgrade() -> None:
    op.drop_column("miner_rewards", "real_prices")
    op.drop_column("miner_rewards", "reward")
