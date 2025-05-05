"""add updated_at created_at for miner_scores

Revision ID: 7ac6116f3b80
Revises: c03e71fa935b
Create Date: 2025-05-02 18:58:59.375937

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "7ac6116f3b80"
down_revision: Union[str, None] = "c03e71fa935b"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "miner_scores",
        sa.Column(
            "created_at",
            sa.DateTime(),
            nullable=False,
            server_default=sa.func.current_timestamp(),
        ),
    )
    op.add_column(
        "miner_scores",
        sa.Column(
            "updated_at",
            sa.DateTime(),
            nullable=False,
            server_default=sa.func.current_timestamp(),
            server_onupdate=sa.func.current_timestamp(),
        ),
    )


def downgrade() -> None:
    op.drop_column("miner_scores", "updated_at")
    op.drop_column("miner_scores", "created_at")
