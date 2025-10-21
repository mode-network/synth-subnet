"""move real paths to prompt

Revision ID: a9227b0cb10b
Revises: 4f05e794f2b2
Create Date: 2025-10-13 22:09:33.896758

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "a9227b0cb10b"
down_revision: Union[str, None] = "4f05e794f2b2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "validator_requests",
        sa.Column("real_prices", sa.JSON(), nullable=True),
    )
    # Migrate data from miner_scores to validator_requests
    op.execute(
        """
        UPDATE validator_requests vr
        SET real_prices = ms.real_prices
        FROM miner_scores ms
        JOIN miner_predictions mp ON ms.miner_predictions_id = mp.id
        WHERE vr.id = mp.validator_requests_id
        """
    )
    # op.drop_column("miner_scores", "real_prices")


def downgrade() -> None:
    # op.add_column(
    #     "miner_scores", sa.Column("real_prices", sa.JSON(), nullable=True)
    # )
    op.drop_column("validator_requests", "real_prices")
