"""remove duplicate prompt score v3

Revision ID: a00eb91223cd
Revises: d45ce8e801b8
Create Date: 2025-05-19 20:56:43.252327

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "a00eb91223cd"
down_revision: Union[str, None] = "d45ce8e801b8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()

    # delete miner scores duplicate on MINER_PREDICTIONS_ID by PROMPT_SCORE_V3, keeping the newer
    conn.execute(
        sa.text(
            """
            DELETE FROM MINER_SCORES
            WHERE
                (MINER_PREDICTIONS_ID, PROMPT_SCORE_V3, ID) IN (
                    SELECT
                        MINER_PREDICTIONS_ID,
                        PROMPT_SCORE_V3,
                        ID
                    FROM
                        (
                            SELECT
                                *,
                                ROW_NUMBER() OVER (
                                    PARTITION BY
                                        MINER_PREDICTIONS_ID
                                    ORDER BY
                                        SCORED_TIME DESC
                                ) AS RN
                            FROM
                                MINER_SCORES
                        ) T
                    WHERE
                        T.RN > 1
                )
            """
        )
    )

    # delete miner scores duplicate on (MINER_PREDICTIONS_ID, PROMPT_SCORE_V3), keeping the older
    conn.execute(
        sa.text(
            """
            DELETE FROM MINER_SCORES
            WHERE
                (MINER_PREDICTIONS_ID, PROMPT_SCORE_V3, ID) IN (
                    SELECT
                        MINER_PREDICTIONS_ID,
                        PROMPT_SCORE_V3,
                        ID
                    FROM
                        (
                            SELECT
                                *,
                                ROW_NUMBER() OVER (
                                    PARTITION BY
                                        MINER_PREDICTIONS_ID,
                                        PROMPT_SCORE_V3
                                    ORDER BY
                                        SCORED_TIME DESC
                                ) AS RN
                            FROM
                                MINER_SCORES
                        ) T
                    WHERE
                        T.RN > 1
                )
            """
        )
    )

    # delete miner scores duplicate on MINER_PREDICTIONS_ID keeping prompt score v3
    conn.execute(
        sa.text(
            """
            delete
            FROM
                MINER_SCORES
            WHERE
                (MINER_PREDICTIONS_ID, id) IN (
                    SELECT
                        MINER_PREDICTIONS_ID, id
                    FROM
                        (
                            SELECT
                                *,
                                ROW_NUMBER() OVER (
                                    PARTITION BY
                                        MINER_PREDICTIONS_ID
                                    ORDER BY
                                        prompt_score_v3 ASC
                                ) AS RN
                            FROM
                                MINER_SCORES
                        ) T
                    WHERE T.RN > 1
            );
            """
        )
    )


def downgrade() -> None:
    pass
