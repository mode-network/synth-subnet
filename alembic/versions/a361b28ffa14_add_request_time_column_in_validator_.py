"""add request_time column in validator_requests table

Revision ID: a361b28ffa14
Revises: a8ca503ab8ec
Create Date: 2025-01-22 14:49:20.011681

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "a361b28ffa14"
down_revision: Union[str, None] = "a8ca503ab8ec"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "validator_requests",
        sa.Column("request_time", sa.TIMESTAMP(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("validator_requests", "request_time")
