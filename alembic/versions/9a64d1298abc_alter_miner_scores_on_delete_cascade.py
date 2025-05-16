"""alter miner scores on delete cascade

Revision ID: 9a64d1298abc
Revises: a24a5f5c0ec8
Create Date: 2025-03-18 12:23:54.686976

"""

from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "9a64d1298abc"
down_revision: Union[str, None] = "a24a5f5c0ec8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_constraint(
        "fk_miner_scores_miner_predictions_id",
        "miner_scores",
        type_="foreignkey",
    )
    op.create_foreign_key(
        constraint_name="fk_miner_scores_miner_predictions_id",
        source_table="miner_scores",
        referent_table="miner_predictions",
        local_cols=["miner_predictions_id"],
        remote_cols=["id"],
        ondelete="CASCADE",
    )


def downgrade() -> None:
    op.drop_constraint(
        "fk_miner_scores_miner_predictions_id",
        "miner_scores",
        type_="foreignkey",
    )
    op.create_foreign_key(
        constraint_name="fk_miner_scores_miner_predictions_id",
        source_table="miner_scores",
        referent_table="miner_predictions",
        local_cols=["miner_predictions_id"],
        remote_cols=["id"],
        ondelete="RESTRICT",
    )
