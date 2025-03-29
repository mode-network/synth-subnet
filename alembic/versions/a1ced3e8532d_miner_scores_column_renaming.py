"""miner_scores column renaming

Revision ID: a1ced3e8532d
Revises: 7eeae448469e
Create Date: 2025-01-08 18:19:58.848962

"""

from typing import Sequence, Union


from alembic import op


# revision identifiers, used by Alembic.
revision: str = "a1ced3e8532d"
down_revision: Union[str, None] = "7eeae448469e"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column("miner_scores", "reward", new_column_name="prompt_score")
    op.alter_column(
        "miner_scores", "reward_details", new_column_name="score_details"
    )


def downgrade() -> None:
    op.alter_column(
        "miner_scores", "score_details", new_column_name="reward_details"
    )
    op.alter_column("miner_scores", "prompt_score", new_column_name="reward")
