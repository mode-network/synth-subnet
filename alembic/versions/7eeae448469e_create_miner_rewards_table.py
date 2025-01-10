"""create miner_rewards table

Revision ID: 7eeae448469e
Revises: 64c3f718c191
Create Date: 2025-01-08 13:01:03.520594

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB, JSON


# revision identifiers, used by Alembic.
revision: str = "7eeae448469e"
down_revision: Union[str, None] = "64c3f718c191"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # remove old table
    op.drop_index(
        "ix_miner_rewards_validation_time", table_name="miner_rewards"
    )
    op.drop_index("ix_miner_rewards_miner_uid", table_name="miner_rewards")
    op.drop_table("miner_rewards")

    # create new table
    op.create_table(
        "miner_rewards",
        sa.Column("id", sa.BigInteger, primary_key=True, nullable=False),
        sa.Column("miner_uid", sa.Integer, nullable=False),
        sa.Column("smoothed_score", sa.Float, nullable=False),
        sa.Column("reward_weight", sa.Float, nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ix_miner_rewards_updated_at", "miner_rewards", ["updated_at"]
    )
    op.create_index(
        "ix_miner_rewards_miner_uid", "miner_rewards", ["miner_uid"]
    )


def downgrade() -> None:
    op.drop_index("ix_miner_rewards_miner_uid", table_name="miner_rewards")
    op.drop_index("ix_miner_rewards_updated_at", table_name="miner_rewards")
    op.drop_table("miner_rewards")

    op.create_table(
        "miner_rewards",
        sa.Column("id", sa.BigInteger, primary_key=True, nullable=False),
        sa.Column("miner_uid", sa.Integer, nullable=False),
        sa.Column("scored_time", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("reward_details", JSONB, nullable=False),
        sa.Column("reward", sa.Float, nullable=True),
        sa.Column("real_prices", JSON, nullable=True),
        sa.Column("prediction", JSON, nullable=True),
    )
    op.create_index(
        "ix_miner_rewards_scored_time", "miner_rewards", ["scored_time"]
    )
    op.create_index(
        "ix_miner_rewards_miner_uid", "miner_rewards", ["miner_uid"]
    )
