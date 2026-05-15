"""index consolidation - drop dead, backfill manual, add missing FK

Revision ID: 668cbf4585eb
Revises: 7d7e8cbdbddd
Create Date: 2026-05-15 10:49:51.415110

Source of truth: wiki/synthdataco/synth-subnet/database/indexes.md.

Live `pg_stat_user_indexes` (mainnet, 2026-05-15) drove the drops:
    ix_metagraph_history_neuron_uid     0 scans, 79 MB
    idx_mp_created_miner                0 scans, 938 MB (duplicated by a
                                                    manual index)
    ix_miner_predictions_miner_uid      1 scan,  209 MB (miner_uid is the
                                                    deprecated denormalized
                                                    column)
    ix_miner_rewards_miner_uid        222 scans, 152 MB
    ix_miner_scores_miner_uid         440 scans, 196 MB

Backfills two indexes that exist in prod but are not in any prior migration
(created manually on the DB at some point). `IF NOT EXISTS` makes the
upgrade a no-op in prod; non-prod environments rebuilt from migrations get
them recreated.

`ix_miner_rewards_miner_id` is genuinely new -- the FK column has no
covering index today, so every leaderboard query that filters or joins on
miner_id does a seq scan of miner_rewards.
"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "668cbf4585eb"
down_revision: Union[str, None] = "7d7e8cbdbddd"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # CREATE/DROP INDEX CONCURRENTLY cannot run inside a transaction.
    op.execute("COMMIT")

    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_miner_rewards_meta_lookup
            ON miner_rewards
            (prompt_name, updated_at, miner_id, reward_weight)
        """)

    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS
            idx_miner_predictions_created_miner
            ON miner_predictions (created_at, miner_id)
        """)

    # --- New: cover the miner_rewards.miner_id FK -------------------------

    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_miner_rewards_miner_id
            ON miner_rewards (miner_id)
        """)

    # --- Drops ------------------------------------------------------------

    # Strictly subsumed by ix_metagraph_history_uid_updated_at
    # (neuron_uid, updated_at DESC) INCLUDE (ip_address).
    op.execute("""
        DROP INDEX CONCURRENTLY IF EXISTS ix_metagraph_history_neuron_uid
        """)

    # Exact duplicate of idx_miner_predictions_created_miner (backfilled
    # above)
    op.execute("""
        DROP INDEX CONCURRENTLY IF EXISTS idx_mp_created_miner
        """)

    # The `miner_uid` column is deprecated.
    # active path is via `miner_id` FK to `miners`.
    op.execute("""
        DROP INDEX CONCURRENTLY IF EXISTS ix_miner_predictions_miner_uid
        """)
    op.execute("""
        DROP INDEX CONCURRENTLY IF EXISTS ix_miner_rewards_miner_uid
        """)
    op.execute("""
        DROP INDEX CONCURRENTLY IF EXISTS ix_miner_scores_miner_uid
        """)


def downgrade() -> None:
    op.execute("COMMIT")

    # Recreate what we dropped.
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_miner_scores_miner_uid
            ON miner_scores (miner_uid)
        """)
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_miner_rewards_miner_uid
            ON miner_rewards (miner_uid)
        """)
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_miner_predictions_miner_uid
            ON miner_predictions (miner_uid)
        """)
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_mp_created_miner
            ON miner_predictions (created_at, miner_id)
        """)
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_metagraph_history_neuron_uid
            ON metagraph_history (neuron_uid)
        """)

    # Drop what we added.
    op.execute("""
        DROP INDEX CONCURRENTLY IF EXISTS ix_miner_rewards_miner_id
        """)
    op.execute("""
        DROP INDEX CONCURRENTLY IF EXISTS idx_miner_predictions_created_miner
        """)
    op.execute("""
        DROP INDEX CONCURRENTLY IF EXISTS idx_miner_rewards_meta_lookup
        """)
