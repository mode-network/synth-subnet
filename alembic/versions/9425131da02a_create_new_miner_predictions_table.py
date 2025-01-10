"""create new miner_predictions table

Revision ID: 9425131da02a
Revises: 9468ab71357e
Create Date: 2024-12-31 18:12:53.722356

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSON


# revision identifiers, used by Alembic.
revision: str = "9425131da02a"
down_revision: Union[str, None] = "9468ab71357e"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "miner_predictions",
        sa.Column("id", sa.BigInteger, primary_key=True),
        sa.Column("validator_requests_id", sa.BigInteger, nullable=False),
        sa.Column("miner_uid", sa.Integer, nullable=False),
        sa.Column("prediction", JSON, nullable=False),
    )
    op.create_foreign_key(
        constraint_name="fk_miner_predictions_validator_requests_id",
        source_table="miner_predictions",
        referent_table="validator_requests",
        local_cols=["validator_requests_id"],
        remote_cols=["id"],
        ondelete="RESTRICT",
    )


def downgrade() -> None:
    op.drop_constraint(
        "fk_miner_predictions_validator_requests_id",
        "miner_predictions",
        type_="foreignkey",
    )
    op.drop_table("miner_predictions")
