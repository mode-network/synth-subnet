import random
from datetime import datetime, timedelta


class PriceDataProvider:
    def __init__(self):
        """Initialize the provider."""
        pass

    def fetch_data(self, start_time, end_time):
        """
        Simulate fetching data from an external REST service.
        Returns an array of time points with random prices.

        :param start_time: ISO 8601 string representing the start time.
        :param end_time: ISO 8601 string representing the end time.
        :return: List of dictionaries with 'time' and 'price' keys.
        """
        # Convert input strings to datetime objects
        start_time = datetime.fromisoformat(start_time)
        end_time = datetime.fromisoformat(end_time)

        if end_time <= start_time:
            raise ValueError("end_time must be greater than start_time")

        # Generate time points at 5-minute intervals between start_time and end_time
        data = []
        current_time = start_time
        while current_time <= end_time:
            data.append({
                "time": current_time.isoformat(),
                "price": round(random.uniform(10, 100), 2)  # Random price
            })
            current_time += timedelta(minutes=5)

        return data
