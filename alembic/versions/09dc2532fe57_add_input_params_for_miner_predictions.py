"""add input params for miner predictions

Revision ID: 09dc2532fe57
Revises: bc6d5957a826
Create Date: 2024-12-26 11:59:36.925991

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "09dc2532fe57"
down_revision: Union[str, None] = "bc6d5957a826"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "miner_predictions", sa.Column("asset", sa.String, nullable=True)
    )
    op.add_column(
        "miner_predictions",
        sa.Column("time_increment", sa.Integer, nullable=True),
    )
    op.add_column(
        "miner_predictions",
        sa.Column("time_length", sa.Integer, nullable=True),
    )
    op.add_column(
        "miner_predictions",
        sa.Column("num_simulations", sa.Integer, nullable=True),
    )


def downgrade() -> None:
    op.drop_column("miner_predictions", "num_simulations")
    op.drop_column("miner_predictions", "time_length")
    op.drop_column("miner_predictions", "time_increment")
    op.drop_column("miner_predictions", "asset")
