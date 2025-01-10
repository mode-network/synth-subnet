"""create miner_scores table

Revision ID: a9177927599a
Revises: 9425131da02a
Create Date: 2024-12-31 19:31:59.207801

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB, JSON


# revision identifiers, used by Alembic.
revision: str = "a9177927599a"
down_revision: Union[str, None] = "9425131da02a"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "validator_scores_prompts",
        sa.Column("id", sa.BigInteger, primary_key=True),
        sa.Column("scored_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("asset", sa.String, nullable=False),
        sa.Column("time_increment", sa.Integer, nullable=False),
        sa.Column("time_length", sa.Integer, nullable=False),
        sa.Column("num_simulations", sa.Integer, nullable=False),
    )

    op.create_table(
        "miner_scores",
        sa.Column("id", sa.BigInteger, primary_key=True, nullable=False),
        sa.Column("miner_uid", sa.Integer, nullable=False),
        sa.Column(
            "validator_scores_prompts_id", sa.BigInteger, nullable=False
        ),
        sa.Column("miner_predictions_id", sa.BigInteger, nullable=True),
        sa.Column("reward", sa.Float, nullable=True),
        sa.Column("reward_details", JSONB, nullable=False),
        sa.Column("real_prices", JSON, nullable=True),
    )
    op.create_foreign_key(
        constraint_name="fk_miner_scores_validator_scores_prompts_id",
        source_table="miner_scores",
        referent_table="validator_scores_prompts",
        local_cols=["validator_scores_prompts_id"],
        remote_cols=["id"],
        ondelete="RESTRICT",
    )
    op.create_foreign_key(
        constraint_name="fk_miner_scores_miner_predictions_id",
        source_table="miner_scores",
        referent_table="miner_predictions",
        local_cols=["miner_predictions_id"],
        remote_cols=["id"],
    )
    op.create_index("ix_miner_scores_miner_uid", "miner_scores", ["miner_uid"])
    op.create_index(
        "ix_validator_scores_prompts_scored_time",
        "validator_scores_prompts",
        ["scored_time"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_validator_scores_prompts_scored_time",
        table_name="validator_scores_prompts",
    )
    op.drop_index("ix_miner_scores_miner_uid", table_name="miner_scores")
    op.drop_constraint(
        "fk_miner_scores_miner_predictions_id",
        "miner_scores",
        type_="foreignkey",
    )
    op.drop_constraint(
        "fk_miner_scores_validator_scores_prompts_id",
        "miner_scores",
        type_="foreignkey",
    )
    op.drop_table("miner_scores")
    op.drop_table("validator_scores_prompts")
