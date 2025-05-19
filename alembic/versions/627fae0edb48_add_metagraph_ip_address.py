"""add metagraph ip address

Revision ID: 627fae0edb48
Revises: 6ebc15f2e397
Create Date: 2025-05-19 16:06:24.751152

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "627fae0edb48"
down_revision: Union[str, None] = "6ebc15f2e397"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "metagraph_history",
        sa.Column("ip_address", sa.String(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("metagraph_history", "ip_address")
