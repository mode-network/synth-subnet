import json
from datetime import datetime, timedelta
import bittensor as bt
from sqlalchemy import select

from simulation.db.models import engine, miner_predictions


class MinerDataHandler:

    @staticmethod
    def set_values(miner_id, start_time: str, values):
        """Set values for the given miner_id and start_time."""

        data = {
            "miner_uid": miner_id,
            "start_time": start_time,
            "prediction": values
        }

        try:
            with engine.connect() as connection:
                with connection.begin():  # Begin a transaction
                    insert_stmt = miner_predictions.insert().values(
                        miner_uid=data["miner_uid"],
                        start_time=data["start_time"],
                        prediction=data["prediction"]
                    )
                    connection.execute(insert_stmt)
        except Exception as e:
            bt.logging.info("in set_values (got an exception): " + str(e))

    @staticmethod
    def get_values(miner_id: int, current_time_str: str):
        """Retrieve the record with the longest valid interval for the given miner_id."""
        current_time = datetime.fromisoformat(current_time_str)

        best_record = None
        max_end_time = current_time - timedelta(days=5)

        with engine.connect() as connection:
            query = select(miner_predictions.c.prediction).where(
                miner_predictions.c.start_time >= max_end_time,
                miner_predictions.c.start_time <= current_time,
                miner_predictions.c.miner_uid == miner_id
            )
            result = connection.execute(query)

            # Fetch all results
            predictions = [row.prediction for row in result]

        bt.logging.info("in get_values, predictions length:" + str(len(predictions)))

        # Find the record with the longest valid interval
        for prediction in predictions:
            if prediction is None:
                continue

            start_time = datetime.fromisoformat(prediction[0]["time"])
            end_time = datetime.fromisoformat(prediction[-1]["time"])

            bt.logging.info("in get_values, first: " + start_time.isoformat())
            bt.logging.info("in get_values, last: " + end_time.isoformat())

            if current_time > end_time:
                if end_time > max_end_time:
                    max_end_time = end_time
                    best_record = prediction

        if not best_record:
            return []

        return best_record
