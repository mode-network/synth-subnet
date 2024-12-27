import unittest
from datetime import datetime
from simulation.validator.miner_data_handler import MinerDataHandler

from tests.utils import generate_values
from simulation.simulation_input import SimulationInput


class TestMinerDataHandler(unittest.TestCase):
    def setUp(self):
        """Set up a temporary file for testing."""
        self.handler = MinerDataHandler()

    def test_get_values_within_range(self):
        """
        Test retrieving values within the valid time range.
        2024-11-20T00:00:00       2024-11-20T23:55:00
                 |-------------------------|                       (Prediction range)

                                                2024-11-22T00:00:00
                                                        |-|        (Current Time)
        """
        miner_id = 1
        start_time = "2024-11-20T00:00:00"
        current_time = "2024-11-22T00:00:00"
        simulation_input = SimulationInput(
            asset="BTC",
            start_time=start_time,
            time_increment=300,
            time_length=86400,
            num_simulations=100
        )

        values = generate_values(datetime.fromisoformat(start_time))
        self.handler.set_values(miner_id, values, simulation_input)

        result = self.handler.get_values(miner_id, current_time)

        self.assertEqual(1, len(result))
        self.assertEqual(288, len(result[0]))  # Half of the 288 intervals
        self.assertEqual({"time": "2024-11-20T12:00:00", "price": 90000}, result[0][0])
        self.assertEqual({"time": "2024-11-21T11:55:00", "price": 233500}, result[0][287])

    def test_get_values_exceeding_range(self):
        """
        Test retrieving values when current_time exceeds the range.
        2024-11-20T00:00:00       2024-11-20T23:55:00
                 |-------------------------|                       (Prediction range)

                                                2024-11-30T00:00:00
                                                        |-|        (Current Time - more than 5 days passed)
        """
        miner_id = 1
        start_time = "2024-11-20T00:00:00"
        current_time = "2024-11-30T00:00:00"
        simulation_input = SimulationInput(
            asset="BTC",
            start_time=start_time,
            time_increment=300,
            time_length=86400,
            num_simulations=100
        )

        values = generate_values(datetime.fromisoformat(start_time))
        self.handler.set_values(miner_id, values, simulation_input)

        result = self.handler.get_values(miner_id, current_time)
        # self.assertEqual(result, []) # TODO: do we want this 5 days expiry?

    def test_get_values_ongoing_range(self):
        """
        Test retrieving values when current_time overlaps with the range.
        2024-11-20T00:00:00       2024-11-20T23:55:00
                 |-------------------------|         (Prediction range)

                    2024-11-20T12:00:00
                            |-|                      (Current Time)
        """
        miner_id = 1
        start_time = "2024-11-20T00:00:00"
        current_time = "2024-11-20T12:00:00"

        simulation_input = SimulationInput(
            asset="BTC",
            start_time=start_time,
            time_increment=300,
            time_length=86400,
            num_simulations=100
        )

        values = generate_values(datetime.fromisoformat(start_time))
        self.handler.set_values(miner_id, values, simulation_input)

        result = self.handler.get_values(miner_id, current_time)
        self.assertEqual(result, [])

    def test_multiple_records_for_same_miner(self):
        """
        Test handling multiple records for the same miner.
        Should take "Prediction range 2" as the latest one

        2024-11-20T00:00:00       2024-11-20T23:55:00
                 |-------------------------|                             (Prediction range 1)

                      2024-11-20T12:00:00       2024-11-21T11:55:00
                               |-------------------------|               (Prediction range 2)

                                                      2024-11-21T15:00:00
                                                              |-|        (Current Time)
        """
        miner_id = 1
        start_time_1 = "2024-11-20T00:00:00"
        start_time_2 = "2024-11-20T12:00:00"
        current_time = "2024-11-21T15:00:00"

        simulation_input1 = SimulationInput(
            asset="BTC",
            start_time=start_time_1,
            time_increment=300,
            time_length=86400,
            num_simulations=100
        )
        simulation_input2 = SimulationInput(
            asset="BTC",
            start_time=start_time_2,
            time_increment=300,
            time_length=86400,
            num_simulations=100
        )

        values = generate_values(datetime.fromisoformat(start_time_1))
        self.handler.set_values(miner_id, values, simulation_input1)

        values = generate_values(datetime.fromisoformat(start_time_2))
        self.handler.set_values(miner_id, values, simulation_input2)

        result = self.handler.get_values(miner_id, current_time)

        self.assertEqual(1, len(result))
        self.assertEqual(288, len(result[0]))  # Half of the 288 intervals
        self.assertEqual({"time": "2024-11-20T12:00:00", "price": 90000}, result[0][0])
        self.assertEqual({"time": "2024-11-21T11:55:00", "price": 233500}, result[0][287])

    def test_multiple_records_for_same_miner_with_overlapping(self):
        """
        Test handling multiple records for the same miner with overlapping records.
        Should take "Prediction range 1" as the latest one

        2024-11-20T00:00:00       2024-11-20T23:55:00
                 |-------------------------|                             (Prediction range 1)

                      2024-11-20T12:00:00       2024-11-21T11:55:00
                               |-------------------------|               (Prediction range 2)

                                        2024-11-21T03:00:00
                                                |-|                      (Current Time)
        """
        miner_id = 1
        start_time_1 = "2024-11-20T00:00:00"
        start_time_2 = "2024-11-20T12:00:00"
        current_time = "2024-11-21T03:00:00"
        simulation_input1 = SimulationInput(
            asset="BTC",
            start_time=start_time_1,
            time_increment=300,
            time_length=86400,
            num_simulations=100
        )

        simulation_input2 = SimulationInput(
            asset="BTC",
            start_time=start_time_2,
            time_increment=300,
            time_length=86400,
            num_simulations=100
        )

        values = generate_values(datetime.fromisoformat(start_time_1))
        self.handler.set_values(miner_id, values, simulation_input1)

        values = generate_values(datetime.fromisoformat(start_time_2))
        self.handler.set_values(miner_id, values, simulation_input2)

        result = self.handler.get_values(miner_id, current_time)

        self.assertEqual(1, len(result))
        self.assertEqual(288, len(result[0]))  # Half of the 288 intervals
        self.assertEqual({"time": "2024-11-20T00:00:00", "price": 90000}, result[0][0])
        self.assertEqual({"time": "2024-11-20T23:55:00", "price": 233500}, result[0][287])

    def test_no_data_for_miner(self):
        """Test retrieving values for a miner that doesn't exist."""
        miner_id = 0
        current_time = "2024-11-20T12:00:00"

        result = self.handler.get_values(miner_id, current_time)
        self.assertEqual(result, [])
