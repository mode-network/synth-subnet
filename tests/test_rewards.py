import unittest
from datetime import datetime


from simulation.validator.miner_data_handler import MinerDataHandler
from simulation.simulation_input import SimulationInput
from simulation.validator.reward import get_rewards, reward


class TestRewards(unittest.TestCase):
    def setUp(self):
        """Set up a temporary file for testing."""
        self.test_file = "test_miner_data.json"
        self.handler = MinerDataHandler(self.test_file)

    def tearDown(self):
        pass

    def test_get_rewards(self):
        start_time = datetime.fromisoformat("2024-11-20T00:00:00")
        current_time = datetime.fromisoformat("2024-11-20T12:00:00")

        softmax_scores = get_rewards(
            self.handler,
            SimulationInput(
                asset="BTC",
                start_time=start_time,
                time_increment=60, # default: 5 mins
                time_length=3600, # default: 1 day
                num_simulations=1 # default: 100
            ),
            [1, 2, 3],
            current_time
        )
        print(softmax_scores)
