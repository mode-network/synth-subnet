"""add index to miner_predictions table and rename start_time column

Revision ID: 8b8ee3e62171
Revises: dba3765d2374
Create Date: 2024-12-08 17:57:30.782720

"""

from typing import Sequence, Union


from alembic import op


# revision identifiers, used by Alembic.
revision: str = "8b8ee3e62171"
down_revision: Union[str, None] = "dba3765d2374"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column(
        "miner_predictions", "start_time", new_column_name="validation_time"
    )
    op.drop_index("ix_start_time", table_name="miner_predictions")
    op.create_index(
        "ix_validation_time", "miner_predictions", ["validation_time"]
    )
    op.create_index("ix_miner_uid", "miner_predictions", ["miner_uid"])


def downgrade() -> None:
    op.drop_index("ix_miner_uid", table_name="miner_predictions")
    op.drop_index("ix_validation_time", table_name="miner_predictions")
    op.alter_column(
        "miner_predictions", "validation_time", new_column_name="start_time"
    )
    op.create_index("ix_start_time", "miner_predictions", ["start_time"])
