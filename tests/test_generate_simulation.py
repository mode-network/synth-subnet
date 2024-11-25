import unittest
from datetime import datetime

from simulation.miner import generate_simulations


class TestGenerateSimulation(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_generate_simulation(self):
        prediction_result = generate_simulations(start_time=datetime.now())

        print(prediction_result)
