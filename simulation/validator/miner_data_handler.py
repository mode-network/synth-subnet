import json
from datetime import datetime, timedelta


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
        with open(self.file_path, 'w') as f:
            json.dump(self.data, f, indent=4)

    def set_values(self, miner_id, start_time, values):
        """Set values for the given miner_id and start_time."""

        # Ensure miner_id exists and append the new record
        if miner_id not in self.data:
            self.data[miner_id] = []
        self.data[miner_id].append({
            "start_time": start_time,
            "values": values
        })
        self._save_data()

    def get_values(self, miner_id, current_time):
        """Retrieve the record with the longest valid interval for the given miner_id."""
        current_time = datetime.fromisoformat(current_time)

        if miner_id not in self.data:
            return []

        best_record = None
        max_interval = timedelta(0)

        # Find the record with the longest valid interval
        for record in self.data[miner_id]:
            start_time = datetime.fromisoformat(record["start_time"])
            end_time = start_time + timedelta(days=1)

            if start_time <= current_time <= end_time:
                interval = current_time - start_time
                if interval > max_interval:
                    max_interval = interval
                    best_record = record

        if not best_record:
            return []

        # Filter and return the values within the interval
        filtered_values = [
            entry for entry in best_record["values"]
            if start_time <= datetime.fromisoformat(entry["time"]) <= current_time
        ]

        return filtered_values
