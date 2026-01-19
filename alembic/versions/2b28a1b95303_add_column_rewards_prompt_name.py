"""add column rewards prompt_name

Revision ID: 2b28a1b95303
Revises: a9227b0cb10b
Create Date: 2025-11-27 17:57:01.394792

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "2b28a1b95303"
down_revision: Union[str, None] = "a9227b0cb10b"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "miner_rewards",
        sa.Column("prompt_name", sa.Text, server_default="low"),
    )


def downgrade() -> None:
    op.drop_column("miner_rewards", "prompt_name")
