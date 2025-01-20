"""add prediction is valid column in predictions table

Revision ID: a8ca503ab8ec
Revises: a1ced3e8532d
Create Date: 2025-01-20 15:50:17.614870

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "a8ca503ab8ec"
down_revision: Union[str, None] = "a1ced3e8532d"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "miner_predictions",
        sa.Column("format_validation", sa.String, nullable=True),
    )


def downgrade() -> None:
    op.drop_column("miner_predictions", "is_format_correct")
