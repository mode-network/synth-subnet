"""remove start_time column and rename validation_time column

Revision ID: bc6d5957a826
Revises: 8b8ee3e62171
Create Date: 2024-12-23 14:45:01.871748

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "bc6d5957a826"
down_revision: Union[str, None] = "8b8ee3e62171"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_column("miner_rewards", "start_time")
    op.alter_column(
        "miner_rewards", "validation_time", new_column_name="start_time"
    )
    op.alter_column(
        "miner_predictions", "validation_time", new_column_name="start_time"
    )


def downgrade() -> None:
    op.alter_column(
        "miner_predictions", "start_time", new_column_name="validation_time"
    )
    op.alter_column(
        "miner_rewards", "start_time", new_column_name="validation_time"
    )
    op.add_column(
        "miner_rewards",
        sa.Column("start_time", sa.TIMESTAMP(timezone=True), nullable=False),
    )
