"""weights_update_history

Revision ID: 97d9545aafd4
Revises: a122779998c1
Create Date: 2025-02-11 11:03:58.061028

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSON


# revision identifiers, used by Alembic.
revision: str = "97d9545aafd4"
down_revision: Union[str, None] = "a122779998c1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "weights_update_history",
        sa.Column("id", sa.BigInteger, primary_key=True, nullable=False),
        sa.Column("miner_uids", JSON, nullable=False),
        sa.Column("miner_weights", JSON, nullable=False),
        sa.Column("update_result", sa.String, nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("weights_update_history")
