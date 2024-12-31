from datetime import datetime
import bittensor as bt
from sqlalchemy import select, join, text

from simulation.db.models import engine, miner_predictions, miner_scores, validator_requests
from simulation.simulation_input import SimulationInput


class MinerDataHandler:

    @staticmethod
    def save_responses(miner_predictions_data: {}, simulation_input: SimulationInput):
        """Save miner predictions and simulation input."""

        validator_requests_row = {
            "start_time": simulation_input.start_time,
            "asset": simulation_input.asset,
            "time_increment": simulation_input.time_increment,
            "time_length": simulation_input.time_length,
            "num_simulations": simulation_input.num_simulations
        }

        try:
            with engine.connect() as connection:
                with connection.begin():  # Begin a transaction
                    insert_stmt_validator_requests = validator_requests.insert().values(validator_requests_row)
                    result = connection.execute(insert_stmt_validator_requests)
                    validator_requests_id = result.inserted_primary_key[0]

                    miner_prediction_records = [
                        {
                            "validator_requests_id": validator_requests_id,
                            "miner_uid": miner_uid,
                            "prediction": prediction
                        }
                        for miner_uid, prediction in miner_predictions_data.items()
                    ]

                    insert_stmt_miner_predictions = miner_predictions.insert().values(miner_prediction_records)
                    connection.execute(insert_stmt_miner_predictions)
        except Exception as e:
            bt.logging.info(f"in set_values (got an exception): {e}")

    @staticmethod
    def set_reward_details(reward_details: [], start_time: str):
        rows_to_insert = [
            {
                "miner_uid": row["miner_uid"],
                "start_time": start_time,
                "reward_details": {
                    "score": row["score"],
                    "softmax_score": row["softmax_score"],
                    "crps_data": row["crps_data"]
                },
                "reward": row["softmax_score"],
                "real_prices": row["real_prices"],
                "prediction": row["predictions"]
            }
            for row in reward_details
        ]

        with engine.begin() as connection:
            try:
                insert_stmt = miner_rewards.insert().values(rows_to_insert)
                connection.execute(insert_stmt)
            except Exception as e:
                connection.rollback()
                bt.logging.info(f"in set_reward_details (got an exception): {e}")

    @staticmethod
    def get_values(miner_uid: int, scored_time_str: str, simulation_input: SimulationInput):
        """Retrieve the record with the longest valid interval for the given miner_id."""
        try:
            scored_time = datetime.fromisoformat(scored_time_str)

            with engine.connect() as connection:
                query = (
                    select(
                        miner_predictions.c.prediction,
                        miner_predictions.c.id
                    )
                    .select_from(miner_predictions)
                    .join(
                        validator_requests,
                        miner_predictions.c.validator_requests_id == validator_requests.c.id
                    )
                    .where(
                        (validator_requests.c.start_time + text(
                            "INTERVAL '1 second'") * miner_predictions.c.time_length) < scored_time,
                        miner_predictions.c.miner_uid == miner_uid,
                        validator_requests.c.asset == simulation_input.asset,
                        validator_requests.c.time_increment == simulation_input.time_increment,
                        validator_requests.c.time_length == simulation_input.time_length,
                        validator_requests.c.num_simulations == simulation_input.num_simulations
                    )
                    .order_by(
                        validator_requests.c.start_time.desc()
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
            bt.logging.info(f"in get_values (got an exception): {e}")
            return None, []
