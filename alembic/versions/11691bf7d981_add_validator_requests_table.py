"""add validator_requests table

Revision ID: 11691bf7d981
Revises: 09dc2532fe57
Create Date: 2024-12-31 17:10:42.389003

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "11691bf7d981"
down_revision: Union[str, None] = "09dc2532fe57"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "validator_requests",
        sa.Column("id", sa.BigInteger, primary_key=True),
        sa.Column("start_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("asset", sa.String, nullable=True),
        sa.Column("time_increment", sa.Integer, nullable=True),
        sa.Column("time_length", sa.Integer, nullable=True),
        sa.Column("num_simulations", sa.Integer, nullable=True),
    )
    op.create_index("ix_start_time", "validator_requests", ["start_time"])


def downgrade() -> None:
    op.drop_index("ix_start_time", table_name="validator_requests")
    op.drop_table("validator_requests")
