"""miner scores unique constraint

Revision ID: c03e71fa935b
Revises: a00eb91223cd
Create Date: 2025-05-02 18:27:07.359860

"""

from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "c03e71fa935b"
down_revision: Union[str, None] = "a00eb91223cd"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_unique_constraint(
        "uq_miner_scores_miner_predictions_id",
        "miner_scores",
        ["miner_predictions_id"],
    )


def downgrade() -> None:
    op.drop_constraint(
        "uq_miner_scores_miner_predictions_id",
        "miner_scores",
        type_="unique",
    )
