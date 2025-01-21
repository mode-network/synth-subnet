from datetime import datetime, timedelta

import bittensor as bt
import pandas as pd
from sqlalchemy import select, text

from simulation.db.models import (
    miner_predictions,
    miner_scores,
    validator_requests,
    metagraph_history,
    miner_rewards,
    get_engine,
)
from simulation.simulation_input import SimulationInput


class MinerDataHandler:
    def __init__(self, engine=None):
        # Use the provided engine or fall back to the default engine
        self.engine = engine or get_engine()

    def save_responses(
        self, miner_predictions_data: {}, simulation_input: SimulationInput
    ):
        """Save miner predictions and simulation input."""

        validator_requests_row = {
            "start_time": simulation_input.start_time,
            "asset": simulation_input.asset,
            "time_increment": simulation_input.time_increment,
            "time_length": simulation_input.time_length,
            "num_simulations": simulation_input.num_simulations,
        }

        try:
            with self.engine.connect() as connection:
                with connection.begin():  # Begin a transaction
                    insert_stmt_validator_requests = (
                        validator_requests.insert().values(
                            validator_requests_row
                        )
                    )
                    result = connection.execute(insert_stmt_validator_requests)
                    validator_requests_id = result.inserted_primary_key[0]

                    miner_prediction_records = [
                        {
                            "validator_requests_id": validator_requests_id,
                            "miner_uid": miner_uid,
                            "prediction": prediction,
                        }
                        for miner_uid, prediction in miner_predictions_data.items()
                    ]

                    insert_stmt_miner_predictions = (
                        miner_predictions.insert().values(
                            miner_prediction_records
                        )
                    )
                    connection.execute(insert_stmt_miner_predictions)
        except Exception as e:
            connection.rollback()
            bt.logging.error(f"in save_responses (got an exception): {e}")

    def set_reward_details(self, reward_details: [], scored_time: str):
        rows_to_insert = [
            {
                "miner_uid": row["miner_uid"],
                "scored_time": scored_time,
                "miner_predictions_id": row["predictions"],
                "score_details": {
                    "score": row["score"],
                    "softmax_score": row["softmax_score"],
                    "crps_data": row["crps_data"],
                },
                "prompt_score": row["softmax_score"],
                "real_prices": row["real_prices"],
            }
            for row in reward_details
        ]

        try:
            with self.engine.connect() as connection:
                with connection.begin():  # Begin a transaction
                    insert_stmt_miner_scores = miner_scores.insert().values(
                        rows_to_insert
                    )
                    connection.execute(insert_stmt_miner_scores)
        except Exception as e:
            connection.rollback()
            bt.logging.error(f"in set_reward_details (got an exception): {e}")

    def get_miner_prediction(self, miner_uid: int, validator_request_id: int):
        """Retrieve the record with the longest valid interval for the given miner_id."""
        try:
            with self.engine.connect() as connection:
                query = (
                    select(
                        miner_predictions.c.id, miner_predictions.c.prediction
                    )
                    .select_from(miner_predictions)
                    .where(
                        miner_predictions.c.miner_uid == miner_uid,
                        miner_predictions.c.validator_requests_id
                        == validator_request_id,
                    )
                    .limit(1)
                )

                result = connection.execute(query).fetchone()

            if result is None:
                return None, []

            record_id = result.id
            prediction = result.prediction

            return record_id, prediction
        except Exception as e:
            bt.logging.error(
                f"in get_miner_prediction (got an exception): {e}"
            )
            return None, []

    def get_latest_prediction_request(
        self, scored_time_str: str, simulation_input: SimulationInput
    ):
        """Retrieve the id of the latest validator request that (start_time + time_length) < scored_time."""
        try:
            scored_time = datetime.fromisoformat(scored_time_str)

            with self.engine.connect() as connection:
                query = (
                    select(validator_requests.c.id)
                    .select_from(validator_requests)
                    .where(
                        (
                            validator_requests.c.start_time
                            + text("INTERVAL '1 second'")
                            * validator_requests.c.time_length
                        )
                        < scored_time,
                        validator_requests.c.asset == simulation_input.asset,
                        validator_requests.c.time_increment
                        == simulation_input.time_increment,
                        validator_requests.c.time_length
                        == simulation_input.time_length,
                        validator_requests.c.num_simulations
                        == simulation_input.num_simulations,
                    )
                    .order_by(validator_requests.c.start_time.desc())
                    .limit(1)
                )

                result = connection.execute(query).fetchone()

                if result is None:
                    return None

                return result.id
        except Exception as e:
            bt.logging.error(
                f"in get_latest_prediction_request (got an exception): {e}"
            )
            return None

    def update_metagraph_history(self, metagraph_info: []):
        try:
            with self.engine.connect() as connection:
                with connection.begin():  # Begin a transaction
                    insert_stmt = metagraph_history.insert().values(
                        metagraph_info
                    )
                    connection.execute(insert_stmt)
        except Exception as e:
            connection.rollback()
            bt.logging.error(
                f"in update_metagraph_history (got an exception): {e}"
            )

    def get_miner_scores(self, scored_time_str: str, cutoff_days: int):
        scored_time = datetime.fromisoformat(scored_time_str)
        min_scored_time = scored_time - timedelta(days=cutoff_days)

        try:
            with self.engine.connect() as connection:
                query = (
                    select(
                        miner_scores.c.miner_uid,
                        miner_scores.c.prompt_score,
                        miner_scores.c.scored_time,
                    )
                    .select_from(miner_scores)
                    .where(miner_scores.c.scored_time > min_scored_time)
                )

                result = connection.execute(query)

            df = pd.DataFrame(result.fetchall(), columns=result.keys())

            return df
        except Exception as e:
            bt.logging.error(f"in get_miner_scores (got an exception): {e}")
            return pd.DataFrame()

    def update_miner_rewards(self, miner_rewards_data: []):
        try:
            with self.engine.connect() as connection:
                with connection.begin():  # Begin a transaction
                    insert_stmt = miner_rewards.insert().values(
                        miner_rewards_data
                    )
                    connection.execute(insert_stmt)
        except Exception as e:
            connection.rollback()
            bt.logging.error(
                f"in update_miner_rewards (got an exception): {e}"
            )
