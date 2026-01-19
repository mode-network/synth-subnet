"""add indexes

Revision ID: 26ab499a7e04
Revises: 2b28a1b95303
Create Date: 2025-12-22 13:56:24.601328

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "26ab499a7e04"
down_revision: Union[str, None] = "2b28a1b95303"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(
        sa.text(
            "CREATE INDEX idx_mp_minerid_validatorid ON miner_predictions (miner_id, validator_requests_id);"
        )
    )
    conn.execute(
        sa.text(
            "CREATE INDEX idx_mp_created_miner ON miner_predictions (created_at, miner_id);"
        )
    )

    conn.execute(
        sa.text(
            "CREATE INDEX idx_vr_id_starttime ON validator_requests (id, start_time DESC);"
        )
    )
    conn.execute(
        sa.text(
            "CREATE INDEX idx_vr_asset_time ON validator_requests(asset, time_increment, time_length);"
        )
    )

    conn.execute(
        sa.text(
            "CREATE INDEX idx_ms_scored_time_predid ON miner_scores(scored_time, miner_predictions_id);"
        )
    )


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(sa.text("DROP INDEX idx_ms_scored_time_predid;"))
    conn.execute(sa.text("DROP INDEX idx_vr_asset_time;"))
    conn.execute(sa.text("DROP INDEX idx_vr_id_starttime;"))
    conn.execute(sa.text("DROP INDEX idx_mp_created_miner;"))
    conn.execute(sa.text("DROP INDEX idx_mp_minerid_validatorid;"))
