"""add miner table

Revision ID: 70f4a9879daf
Revises: 9a64d1298abc
Create Date: 2025-03-19 12:45:04.674078

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "70f4a9879daf"
down_revision: Union[str, None] = "9a64d1298abc"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "miners",
        sa.Column("id", sa.BigInteger(), nullable=False, autoincrement=True),
        sa.Column("miner_uid", sa.Integer(), nullable=False),
        sa.Column("coldkey", sa.String(), nullable=True),
        sa.Column("hotkey", sa.String(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(),
            nullable=False,
            server_default=sa.func.current_timestamp(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            nullable=False,
            server_default=sa.func.current_timestamp(),
            server_onupdate=sa.func.current_timestamp(),
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("miners_miner_uid", "miners", ["miner_uid"])
    op.create_unique_constraint(
        "uq_miners_miner_uid_coldkey_hotkey",
        "miners",
        ["miner_uid", "coldkey", "hotkey"],
    )

    # add columns
    op.add_column(
        "miner_predictions",
        sa.Column("miner_id", sa.BigInteger(), nullable=True),
    )
    op.add_column(
        "miner_rewards", sa.Column("miner_id", sa.BigInteger(), nullable=True)
    )

    # populate the miner table based on the metagraph_history
    op.execute(
        """
        INSERT INTO miners (miner_uid, coldkey, hotkey)
        with ranked_neurons as (
            SELECT neuron_uid, coldkey, hotkey, updated_at, ROW_NUMBER () OVER (PARTITION by neuron_uid ORDER BY updated_at desc) as rn
            FROM metagraph_history
        )
        select neuron_uid as miner_uid, coldkey, hotkey
        from ranked_neurons
        where rn = 1
        """
    )

    # populate other tables
    op.execute(
        """
        UPDATE miner_predictions
        SET miner_id = miners.id
        FROM miners
        WHERE miner_predictions.miner_uid = miners.miner_uid
        """
    )
    op.execute(
        """
        UPDATE miner_rewards
        SET miner_id = miners.id
        FROM miners
        WHERE miner_rewards.miner_uid = miners.miner_uid
        """
    )

    # create the foreign key constraints
    op.create_foreign_key(
        "fk_miner_predictions_miner_id",
        "miner_predictions",
        "miners",
        ["miner_id"],
        ["id"],
    )
    op.create_foreign_key(
        "fk_miner_rewards_miner_id",
        "miner_rewards",
        "miners",
        ["miner_id"],
        ["id"],
    )

    # alter columns miner_id to be not nullable
    # op.alter_column(
    #     "miner_predictions",
    #     "miner_id",
    #     existing_type=sa.BigInteger(),
    #     nullable=False,
    # )
    # op.alter_column(
    #     "miner_rewards",
    #     "miner_id",
    #     existing_type=sa.BigInteger(),
    #     nullable=False,
    # )

    # alter columns miner_uid to be nullable
    op.alter_column(
        "miner_predictions",
        "miner_uid",
        existing_type=sa.BigInteger(),
        nullable=True,
    )
    op.alter_column(
        "miner_rewards",
        "miner_uid",
        existing_type=sa.BigInteger(),
        nullable=True,
    )

    # commented because no destructive operation for now

    # remove useless column (we can get it with miner_predictions_id -> miner_id -> miner_uid)
    # op.drop_column('miner_scores', 'miner_uid')

    # remove columns miner_uid
    # op.drop_column('miner_predictions', 'miner_uid')
    # op.drop_column('miner_rewards', 'miner_uid')


def downgrade() -> None:
    # commented because no destructive operation for now so no need to revert

    # # add columns
    # op.add_column(
    #     "miner_predictions",
    #     sa.Column("miner_uid", sa.BigInteger(), nullable=True),
    # )
    # op.add_column(
    #     "miner_rewards",
    #     sa.Column("miner_uid", sa.BigInteger(), nullable=True),
    # )

    # # populate the miner_uid column
    # op.execute(
    #     """
    #     UPDATE miner_predictions
    #     SET miner_uid = miners.miner_uid
    #     FROM miners
    #     WHERE miner_predictions.miner_id = miners.id
    #     """
    # )
    # op.execute(
    #     """
    #     UPDATE miner_rewards
    #     SET miner_uid = miners.miner_uid
    #     FROM miners
    #     WHERE miner_rewards.miner_id = miners.id
    #     """
    # )

    # remove the foreign key constraints
    op.drop_constraint(
        "fk_miner_predictions_miner_id",
        "miner_predictions",
        type_="foreignkey",
    )
    op.drop_constraint(
        "fk_miner_rewards_miner_id", "miner_rewards", type_="foreignkey"
    )

    # remove the columns
    op.drop_column("miner_predictions", "miner_id")
    op.drop_column("miner_rewards", "miner_id")

    # drop the miners table
    op.drop_index("miners_miner_uid", table_name="miners")
    op.drop_table("miners")
