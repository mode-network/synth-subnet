"""add index on miner_predictions

Revision ID: 3ae76bddd86f
Revises: e00913dea20f
Create Date: 2025-02-25 16:34:06.515580

"""

from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "3ae76bddd86f"
down_revision: Union[str, None] = "e00913dea20f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_index(
        "ix_miner_predictions_miner_uid", "miner_predictions", ["miner_uid"]
    )


def downgrade() -> None:
    op.drop_index(
        "ix_miner_predictions_miner_uid", table_name="miner_predictions"
    )
