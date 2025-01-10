"""remove column from miner_scores and validator_scores_prompts table

Revision ID: f45a0fe27a22
Revises: 6778da854170
Create Date: 2025-01-03 19:55:35.633424

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "f45a0fe27a22"
down_revision: Union[str, None] = "6778da854170"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_constraint(
        "fk_miner_scores_validator_scores_prompts_id",
        "miner_scores",
        type_="foreignkey",
    )
    op.drop_column("miner_scores", "validator_scores_prompts_id")
    op.drop_index(
        "ix_validator_scores_prompts_scored_time",
        table_name="validator_scores_prompts",
    )
    op.drop_table("validator_scores_prompts")


def downgrade() -> None:
    op.create_table(
        "validator_scores_prompts",
        sa.Column("id", sa.BigInteger, primary_key=True),
        sa.Column("scored_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("asset", sa.String, nullable=False),
        sa.Column("time_increment", sa.Integer, nullable=False),
        sa.Column("time_length", sa.Integer, nullable=False),
        sa.Column("num_simulations", sa.Integer, nullable=False),
    )
    op.create_index(
        "ix_validator_scores_prompts_scored_time",
        "validator_scores_prompts",
        ["scored_time"],
    )
    op.add_column(
        "miner_scores",
        sa.Column(
            "validator_scores_prompts_id", sa.BigInteger, nullable=False
        ),
    )
    op.create_foreign_key(
        constraint_name="fk_miner_scores_validator_scores_prompts_id",
        source_table="miner_scores",
        referent_table="validator_scores_prompts",
        local_cols=["validator_scores_prompts_id"],
        remote_cols=["id"],
        ondelete="RESTRICT",
    )
