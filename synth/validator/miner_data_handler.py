from datetime import datetime, timedelta

import bittensor as bt
import pandas as pd
from sqlalchemy import select, text

from synth.db.models import (
    miner_predictions as miner_predictions_model,
    miner_scores,
    validator_requests,
    metagraph_history,
    miner_rewards,
    get_engine,
    weights_update_history,
)
from synth.simulation_input import SimulationInput
from synth.validator import response_validation


class MinerDataHandler:
    def __init__(self, engine=None):
        # Use the provided engine or fall back to the default engine
        self.engine = engine or get_engine()

    def save_responses(
        self,
        miner_predictions: dict[tuple],
        simulation_input: SimulationInput,
        request_time: datetime,
    ):
        """Save miner predictions and simulation input."""

        validator_requests_row = {
            "start_time": simulation_input.start_time,
            "asset": simulation_input.asset,
            "time_increment": simulation_input.time_increment,
            "time_length": simulation_input.time_length,
            "num_simulations": simulation_input.num_simulations,
            "request_time": request_time.isoformat(),
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

                    miner_prediction_records = []
                    for miner_uid, (
                        prediction,
                        format_validation,
                        process_time,
                    ) in miner_predictions.items():
                        # If the format is not correct, we don't save the prediction
                        if format_validation != response_validation.CORRECT:
                            prediction = []

                        miner_prediction_records.append(
                            {
                                "validator_requests_id": validator_requests_id,
                                "miner_uid": miner_uid,
                                "prediction": prediction,
                                "format_validation": format_validation,
                                "process_time": process_time,
                            }
                        )

                    insert_stmt_miner_predictions = (
                        miner_predictions_model.insert().values(
                            miner_prediction_records
                        )
                    )
                    connection.execute(insert_stmt_miner_predictions)
            # return validator_requests_id # TODO: finish this: refactor to add the validator_requests_id in the score and reward table
        except Exception as e:
            connection.rollback()
            bt.logging.error(f"in save_responses (got an exception): {e}")

    def set_reward_details(self, reward_details: list[dict], scored_time: str):
        rows_to_insert = [
            {
                "miner_uid": row["miner_uid"],
                "scored_time": scored_time,
                "miner_predictions_id": row["miner_prediction_id"],
                "score_details_v2": {
                    "total_crps": row["total_crps"],
                    "percentile90": row["percentile90"],
                    "lowest_score": row["lowest_score"],
                    "prompt_score_v2": row["prompt_score_v2"],
                    "crps_data": row["crps_data"],
                },
                "prompt_score_v2": row["prompt_score_v2"],
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
                        miner_predictions_model.c.id,
                        miner_predictions_model.c.prediction,
                        miner_predictions_model.c.format_validation,
                    )
                    .select_from(miner_predictions_model)
                    .where(
                        miner_predictions_model.c.miner_uid == miner_uid,
                        miner_predictions_model.c.validator_requests_id
                        == validator_request_id,
                    )
                    .limit(1)
                )

                result = connection.execute(query).fetchone()

            if result is None:
                return None, [], ""

            record_id = result.id
            prediction = result.prediction
            format_validation = result.format_validation

            return record_id, prediction, format_validation
        except Exception as e:
            bt.logging.error(
                f"in get_miner_prediction (got an exception): {e}"
            )
            return None, [], ""

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
            if connection:
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
                        miner_scores.c.prompt_score_v2,
                        miner_scores.c.scored_time,
                        miner_scores.c.score_details_v2,
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

    def update_miner_rewards(self, miner_rewards_data: list[dict]):
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

    def update_weights_history(
        self,
        miner_uids: list[int],
        miner_weights: list[float],
        norm_miner_uids: list[int],
        norm_miner_weights: list[int],
        update_result: str,
        scored_time: str,
    ):
        update_weights_rows = {
            "miner_uids": miner_uids,
            "miner_weights": miner_weights,
            "norm_miner_uids": norm_miner_uids,
            "norm_miner_weights": norm_miner_weights,
            "update_result": update_result,
            "updated_at": scored_time,
        }

        try:
            with self.engine.connect() as connection:
                with connection.begin():  # Begin a transaction
                    insert_stmt = weights_update_history.insert().values(
                        update_weights_rows
                    )
                    connection.execute(insert_stmt)
        except Exception as e:
            connection.rollback()
            bt.logging.error(
                f"in update_weights_history (got an exception): {e}"
            )
