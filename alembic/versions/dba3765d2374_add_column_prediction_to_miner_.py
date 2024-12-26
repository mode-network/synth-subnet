"""add column prediction to miner_predictions table

Revision ID: dba3765d2374
Revises: 448fada07788
Create Date: 2024-12-08 17:36:07.405120

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSON


# revision identifiers, used by Alembic.
revision: str = "dba3765d2374"
down_revision: Union[str, None] = "448fada07788"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "miner_rewards", sa.Column("prediction", JSON, nullable=True)
    )


def downgrade() -> None:
    op.drop_column("miner_rewards", "prediction")
