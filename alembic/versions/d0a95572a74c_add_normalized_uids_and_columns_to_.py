"""add normalized uids and columns to weights_update_history

Revision ID: d0a95572a74c
Revises: 97d9545aafd4
Create Date: 2025-02-12 11:27:08.264735

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSON


# revision identifiers, used by Alembic.
revision: str = "d0a95572a74c"
down_revision: Union[str, None] = "97d9545aafd4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "weights_update_history",
        sa.Column("norm_miner_uids", JSON, nullable=True),
    )
    op.add_column(
        "weights_update_history",
        sa.Column("norm_miner_weights", JSON, nullable=True),
    )


def downgrade() -> None:
    op.drop_column("weights_update_history", "norm_miner_weights")
    op.drop_column("weights_update_history", "norm_miner_uids")
