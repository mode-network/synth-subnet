"""delete old predictions table

Revision ID: a24a5f5c0ec8
Revises: 4cecf5cce9ca
Create Date: 2025-03-18 12:03:12.943097

"""

from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "a24a5f5c0ec8"
down_revision: Union[str, None] = "4cecf5cce9ca"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_table("miner_predictions_old")


def downgrade() -> None:
    pass
