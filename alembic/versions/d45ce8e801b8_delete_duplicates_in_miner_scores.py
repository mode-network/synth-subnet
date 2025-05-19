"""delete duplicates in miner scores

Revision ID: d45ce8e801b8
Revises: c5b7c635f0a8
Create Date: 2025-05-02 18:26:59.061957

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "d45ce8e801b8"
down_revision: Union[str, None] = "c5b7c635f0a8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # delete miner_scores where miner_predictions_id is null
    conn = op.get_bind()
    conn.execute(
        sa.text(
            """
            DELETE FROM miner_scores
            WHERE miner_predictions_id IS NULL
            """
        )
    )

    # delete miner scores duplicate on (MINER_PREDICTIONS_ID, PROMPT_SCORE), keeping the older
    conn.execute(
        sa.text(
            """
DELETE
FROM
    MINER_SCORES
WHERE
    (MINER_PREDICTIONS_ID, PROMPT_SCORE, ID) IN (
        SELECT
            MINER_PREDICTIONS_ID,
            PROMPT_SCORE,
            ID
        FROM
            (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            MINER_PREDICTIONS_ID,
                            PROMPT_SCORE
                        ORDER BY
                            SCORED_TIME asc
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

    # delete miner scores duplicate on MINER_PREDICTIONS_ID by PROMPT_SCORE, keeping the newer
    conn.execute(
        sa.text(
            """
DELETE FROM MINER_SCORES
WHERE
    (MINER_PREDICTIONS_ID, PROMPT_SCORE, ID) IN (
        SELECT
            MINER_PREDICTIONS_ID,
            PROMPT_SCORE,
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

    # delete miner scores duplicate on MINER_PREDICTIONS_ID by PROMPT_SCORE_V2, keeping the newer
    conn.execute(
        sa.text(
            """
DELETE FROM MINER_SCORES
WHERE
    (MINER_PREDICTIONS_ID, PROMPT_SCORE_V2, ID) IN (
        SELECT
            MINER_PREDICTIONS_ID,
            PROMPT_SCORE_V2,
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

    # delete miner scores duplicate on (MINER_PREDICTIONS_ID, PROMPT_SCORE_V2), keeping the older
    conn.execute(
        sa.text(
            """
DELETE FROM MINER_SCORES
WHERE
    (MINER_PREDICTIONS_ID, PROMPT_SCORE_V2, ID) IN (
        SELECT
            MINER_PREDICTIONS_ID,
            PROMPT_SCORE_V2,
            ID
        FROM
            (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            MINER_PREDICTIONS_ID,
                            PROMPT_SCORE_V2
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


def downgrade() -> None:
    pass
