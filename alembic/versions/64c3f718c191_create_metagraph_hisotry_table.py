"""create metagraph_hisotry table

Revision ID: 64c3f718c191
Revises: f45a0fe27a22
Create Date: 2025-01-07 14:54:29.448230

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "64c3f718c191"
down_revision: Union[str, None] = "f45a0fe27a22"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "metagraph_history",
        sa.Column("id", sa.BigInteger, primary_key=True, nullable=False),
        sa.Column("neuron_uid", sa.Integer, nullable=False),
        sa.Column("incentive", sa.Float, nullable=True),
        sa.Column("rank", sa.Float, nullable=True),
        sa.Column("stake", sa.Float, nullable=True),
        sa.Column("trust", sa.Float, nullable=True),
        sa.Column("emission", sa.Float, nullable=True),
        sa.Column("coldkey", sa.String, nullable=True),
        sa.Column("hotkey", sa.String, nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ix_metagraph_history_updated_at", "metagraph_history", ["updated_at"]
    )
    op.create_index(
        "ix_metagraph_history_neuron_uid", "metagraph_history", ["neuron_uid"]
    )


def downgrade() -> None:
    op.drop_index(
        "ix_metagraph_history_neuron_uid", table_name="metagraph_history"
    )
    op.drop_index(
        "ix_metagraph_history_updated_at", table_name="metagraph_history"
    )
    op.drop_table("metagraph_history")
