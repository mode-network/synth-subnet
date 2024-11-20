import unittest
import os
from datetime import datetime, timedelta
from simulation.validator.miner_data_handler import MinerDataHandler


def generate_values(start_time):
    """Generate values for the given miner_id and start_time."""
    start_time = datetime.fromisoformat(start_time)
    values = []
    for i in range(0, 24 * 60, 5):  # 5-minute intervals for 1 day
        time_point = (start_time + timedelta(minutes=i)).isoformat()
        price = 90000 + i * 100  # Random price
        values.append({"time": time_point, "price": price})

    return values


class TestMinerDataHandler(unittest.TestCase):
    def setUp(self):
        """Set up a temporary file for testing."""
        self.test_file = "test_miner_data.json"
        self.handler = MinerDataHandler(self.test_file)

    def tearDown(self):
        """Clean up the temporary file after each test."""
        if os.path.exists(self.test_file):
            os.remove(self.test_file)

    def test_get_values_within_range(self):
        """Test retrieving values within the valid time range."""
        miner_id = "miner_123"
        start_time = "2024-11-20T00:00:00"
        current_time = "2024-11-20T12:00:00"

        values = generate_values(start_time)
        self.handler.set_values(miner_id, start_time, values)

        result = self.handler.get_values(miner_id, current_time)

        self.assertEqual(145, len(result))  # Half of the 288 intervals
        self.assertEqual({"time": "2024-11-20T00:00:00", "price": 90000}, result[0])
        self.assertEqual({"time": "2024-11-20T12:00:00", "price": 162000}, result[144])

    def test_get_values_exceeding_range(self):
        """Test retrieving values when current_time exceeds the range."""
        miner_id = "miner_123"
        start_time = "2024-11-20T00:00:00"
        current_time = "2024-11-22T00:00:00"

        values = generate_values(start_time)
        self.handler.set_values(miner_id, start_time, values)

        result = self.handler.get_values(miner_id, current_time)
        self.assertEqual(result, [])

    def test_multiple_records_for_same_miner(self):
        """Test handling multiple records for the same miner."""
        miner_id = "miner_123"
        start_time_1 = "2024-11-20T00:00:00"
        start_time_2 = "2024-11-21T00:00:00"
        current_time = "2024-11-21T12:00:00"

        values = generate_values(start_time_1)
        self.handler.set_values(miner_id, start_time_1, values)

        values = generate_values(start_time_2)
        self.handler.set_values(miner_id, start_time_2, values)

        result = self.handler.get_values(miner_id, current_time)

        # Should return values from the second record, as it's the longest valid interval
        self.assertTrue(all(start_time_2 <= value["time"] <= current_time for value in result))

    def test_no_data_for_miner(self):
        """Test retrieving values for a miner that doesn't exist."""
        miner_id = "nonexistent_miner"
        current_time = "2024-11-20T12:00:00"

        result = self.handler.get_values(miner_id, current_time)
        self.assertEqual(result, [])
