import json
from datetime import datetime, timedelta
import bittensor as bt


class MinerDataHandler:
    def __init__(self, file_path):
        self.file_path = file_path
        self.data = self._load_data()

    def _load_data(self):
        """Load data from the file if it exists, otherwise return an empty dictionary."""
        try:
            with open(self.file_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            return {}

    def _save_data(self):
        """Save data to the file."""
        # bt.logging.debug("data for saving in the file: " + str(self.data))
        with open(self.file_path, 'w') as f:
            json.dump(self.data, f, indent=4)

    def set_values(self, miner_id, start_time: str, values):
        """Set values for the given miner_id and start_time."""

        miner_id_str = str(miner_id)

        # Ensure miner_id exists and append the new record
        bt.logging.info("before: " + str(self.data))
        if miner_id_str not in self.data:
            self.data[miner_id_str] = []
        self.data[miner_id_str].append({
            "start_time": start_time,
            "values": values
        })
        bt.logging.info("after: " + str(self.data))
        self._save_data()

    def get_values(self, miner_id, current_time_str: str):
        """Retrieve the record with the longest valid interval for the given miner_id."""
        miner_id_str = str(miner_id)

        if miner_id_str not in self.data:
            return []

        current_time = datetime.fromisoformat(current_time_str)

        best_record = None
        max_end_time = current_time - timedelta(days=5)

        bt.logging.info("in get_values: miner_id is " + miner_id_str)

        # Find the record with the longest valid interval
        for record in self.data[miner_id_str]:
            if record["values"] is None:
                continue

            start_time = datetime.fromisoformat(record["values"][0]["time"])
            end_time = datetime.fromisoformat(record["values"][-1]["time"])

            bt.logging.info("in get_values, first: " + start_time.isoformat())
            bt.logging.info("in get_values, last: " + end_time.isoformat())

            if current_time > end_time:
                if end_time > max_end_time:
                    max_end_time = end_time
                    best_record = record

        bt.logging.info("in get_values: best_record is " + str(best_record))

        if not best_record:
            return []

        if best_record["values"] is None:
            return []

        # Filter and return the values within the interval
        filtered_values = [
            entry for entry in best_record["values"]
        ]

        return filtered_values
