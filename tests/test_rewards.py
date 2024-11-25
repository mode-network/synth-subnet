import unittest
from datetime import datetime


from simulation.validator.miner_data_handler import MinerDataHandler
from simulation.simulation_input import SimulationInput
from simulation.validator.price_data_provider import PriceDataProvider
from simulation.validator.reward import get_rewards
from tests.utils import generate_values


class TestRewards(unittest.TestCase):
    def setUp(self):
        """Set up a temporary file for testing."""
        self.test_file = "test_miner_data.json"
        self.handler = MinerDataHandler(self.test_file)
        self.price_data_provider = PriceDataProvider("BTC", "2024-11-25T00:00:00")

    def tearDown(self):
        pass

    def test_get_rewards(self):
        miner_id = 0
        start_time = "2024-11-20T00:00:00"
        current_time = "2024-11-20T12:00:00"

        values = generate_values(datetime.fromisoformat(start_time))
        self.handler.set_values(miner_id, start_time, values)

        softmax_scores = get_rewards(
            self.handler,
            self.price_data_provider,
            SimulationInput(
                asset="BTC",
                start_time=start_time,
                time_increment=60, # default: 5 mins
                time_length=3600, # default: 1 day
                num_simulations=1 # default: 100
            ),
            [miner_id],  # TODO: add another test with more miners
            current_time
        )
        print(softmax_scores)
        # TODO: assert the scores
