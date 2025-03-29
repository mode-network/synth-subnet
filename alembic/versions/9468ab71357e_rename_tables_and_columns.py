"""rename tables and columns

Revision ID: 9468ab71357e
Revises: 11691bf7d981
Create Date: 2024-12-31 17:56:40.448935

"""

from typing import Sequence, Union


from alembic import op


# revision identifiers, used by Alembic.
revision: str = "9468ab71357e"
down_revision: Union[str, None] = "11691bf7d981"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.rename_table("miner_predictions", "miner_predictions_old")
    op.alter_column(
        "miner_rewards", "start_time", new_column_name="scored_time"
    )


def downgrade() -> None:
    op.alter_column(
        "miner_rewards", "scored_time", new_column_name="start_time"
    )
    op.rename_table("miner_predictions_old", "miner_predictions")
