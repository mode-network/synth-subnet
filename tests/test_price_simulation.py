import unittest
import numpy as np

from simulation.simulations.price_simulation import simulate_crypto_price_paths


class TestPriceSimulation(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_simulate_crypto_price_paths(self):
        # Simulation parameters
        current_price = 100.0
        time_increment = 300  # 5 minutes
        time_length = 1800  # 30 min
        num_simulations = 1

        # Real price path sigma
        sigma_real = 0.01

        price_paths = simulate_crypto_price_paths(
            current_price, time_increment, time_length, num_simulations, sigma_real
        )

        # Test the shape of the result
        self.assertEqual(price_paths.shape, (num_simulations, time_length / time_increment + 1))

        # Test that all values are finite (no NaN or Inf values)
        self.assertTrue(np.all(np.isfinite(price_paths)))

        # Check that the initial price in each path matches the given current price
        for path in price_paths:
            self.assertAlmostEqual(path[0], current_price, delta=1e-6)

        # Additional property tests can go here (e.g., variance checks based on sigma)
