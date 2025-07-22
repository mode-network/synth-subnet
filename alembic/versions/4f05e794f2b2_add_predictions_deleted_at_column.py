"""add predictions deleted at column

Revision ID: 4f05e794f2b2
Revises: c03e71fa935b
Create Date: 2025-07-21 15:39:19.531463

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "4f05e794f2b2"
down_revision: Union[str, None] = "c03e71fa935b"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "miner_predictions",
        sa.Column("deleted_at", sa.DateTime(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("miner_predictions", "deleted_at")
