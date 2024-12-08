import json
from datetime import datetime, timedelta
import bittensor as bt
from sqlalchemy import select

from simulation.db.models import engine, miner_predictions, miner_rewards


class MinerDataHandler:

    @staticmethod
    def set_values(miner_uid, validation_time: str, values):
        """Set values for the given miner_id and validation_time."""

        data = {
            "miner_uid": miner_uid,
            "validation_time": validation_time,
            "prediction": values
        }

        try:
            with engine.connect() as connection:
                with connection.begin():  # Begin a transaction
                    insert_stmt = miner_predictions.insert().values(
                        miner_uid=data["miner_uid"],
                        validation_time=data["validation_time"],
                        prediction=data["prediction"]
                    )
                    connection.execute(insert_stmt)
        except Exception as e:
            bt.logging.info(f"in set_values (got an exception): {e}")

    @staticmethod
    def set_reward_details(reward_details: [], validation_time: str, start_time: str):
        rows_to_insert = [
            {
                "miner_uid": row["miner_uid"],
                "validation_time": validation_time,
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
    def get_values(miner_uid: int, current_time_str: str):
        """Retrieve the record with the longest valid interval for the given miner_id."""
        current_time = datetime.fromisoformat(current_time_str)

        best_record = None
        max_end_time = current_time - timedelta(days=5)

        with engine.connect() as connection:
            query = select(miner_predictions.c.prediction).where(
                miner_predictions.c.validation_time >= max_end_time,
                miner_predictions.c.validation_time <= current_time,
                miner_predictions.c.miner_uid == miner_uid
            )
            result = connection.execute(query)

            # Fetch all results
            predictions = [row.prediction for row in result]

        bt.logging.info("in get_values, predictions length:" + str(len(predictions)))

        # Find the record with the longest valid interval
        for prediction in predictions:
            if prediction is None:
                continue

            end_time = datetime.fromisoformat(prediction[-1]["time"])

            if current_time > end_time:
                if end_time > max_end_time:
                    max_end_time = end_time
                    best_record = prediction

        if not best_record:
            return []

        return best_record
