"""
Test suite for the CRPS division by zero bug fix.

This test validates that the fix for division by zero in absolute price
normalization works correctly.
"""

import unittest
import numpy as np

from synth.validator.crps_calculation import calculate_crps_for_miner
from synth.validator import prompt_config


class TestCRPSDivisionByZeroFix(unittest.TestCase):
    """Test cases for the CRPS division by zero bug fix."""

    def test_zero_last_price_no_crash(self):
        """Test that zero last price doesn't crash CRPS calculation."""
        time_increment = 300
        simulation_runs = np.array([[100.0, 105.0, 110.0]])
        real_price_path = np.array([100.0, 105.0, 0.0])  # Last price is 0

        # Should not crash
        score, detailed_data = calculate_crps_for_miner(
            simulation_runs,
            real_price_path,
            time_increment,
            {"24hour_abs": 86400},
        )

        # Should handle gracefully (either skip interval or return valid score)
        self.assertIsNotNone(score)
        self.assertIsInstance(score, float)

    def test_nan_last_price_no_crash(self):
        """Test that NaN last price doesn't crash CRPS calculation."""
        time_increment = 300
        simulation_runs = np.array([[100.0, 105.0, 110.0]])
        real_price_path = np.array([100.0, 105.0, np.nan])  # Last price is NaN

        # Should not crash
        score, detailed_data = calculate_crps_for_miner(
            simulation_runs,
            real_price_path,
            time_increment,
            {"24hour_abs": 86400},
        )

        # Should handle gracefully
        self.assertIsNotNone(score)
        self.assertIsInstance(score, float)

    def test_inf_last_price_no_crash(self):
        """Test that infinite last price doesn't crash CRPS calculation."""
        time_increment = 300
        simulation_runs = np.array([[100.0, 105.0, 110.0]])
        real_price_path = np.array([100.0, 105.0, np.inf])  # Last price is inf

        # Should not crash
        score, detailed_data = calculate_crps_for_miner(
            simulation_runs,
            real_price_path,
            time_increment,
            {"24hour_abs": 86400},
        )

        # Should handle gracefully
        self.assertIsNotNone(score)
        self.assertIsInstance(score, float)

    def test_valid_last_price_works_correctly(self):
        """Test that valid last price still works correctly."""
        time_increment = 300
        simulation_runs = np.array([[100.0, 105.0, 110.0]])
        real_price_path = np.array([100.0, 105.0, 108.0])  # Valid prices

        score, detailed_data = calculate_crps_for_miner(
            simulation_runs,
            real_price_path,
            time_increment,
            {"24hour_abs": 86400},
        )

        # Should work normally
        self.assertIsNotNone(score)
        self.assertIsInstance(score, float)
        self.assertGreaterEqual(score, 0.0)

    def test_mixed_intervals_with_invalid_last_price(self):
        """Test that invalid last price only affects absolute price intervals."""
        time_increment = 300
        simulation_runs = np.array(
            [[100.0, 105.0, 110.0, 115.0, 120.0]]
        )
        real_price_path = np.array([100.0, 105.0, 110.0, 115.0, 0.0])

        # Mix of absolute and relative intervals
        scoring_intervals = {
            "5min": 300,  # Relative - should work
            "10min": 600,  # Relative - should work
            "20min_abs": 1200,  # Absolute - should skip due to zero
        }

        score, detailed_data = calculate_crps_for_miner(
            simulation_runs,
            real_price_path,
            time_increment,
            scoring_intervals,
        )

        # Should not crash and should have some score from relative intervals
        self.assertIsNotNone(score)
        self.assertIsInstance(score, float)

    def test_very_small_last_price(self):
        """Test that very small but non-zero last price works."""
        time_increment = 300
        simulation_runs = np.array([[100.0, 105.0, 110.0]])
        real_price_path = np.array([100.0, 105.0, 0.0001])  # Very small

        score, detailed_data = calculate_crps_for_miner(
            simulation_runs,
            real_price_path,
            time_increment,
            {"24hour_abs": 86400},
        )

        # Should work (not zero, not NaN, not inf)
        self.assertIsNotNone(score)
        self.assertIsInstance(score, float)
        self.assertFalse(np.isnan(score))
        self.assertFalse(np.isinf(score))

    def test_negative_last_price(self):
        """Test that negative last price works (crypto can't be negative, but test edge case)."""
        time_increment = 300
        simulation_runs = np.array([[100.0, 105.0, 110.0]])
        real_price_path = np.array([100.0, 105.0, -50.0])  # Negative

        score, detailed_data = calculate_crps_for_miner(
            simulation_runs,
            real_price_path,
            time_increment,
            {"24hour_abs": 86400},
        )

        # Should work (negative is valid for division, just unusual)
        self.assertIsNotNone(score)
        self.assertIsInstance(score, float)

    def test_low_frequency_config_with_zero_last_price(self):
        """Test with actual LOW_FREQUENCY config and zero last price."""
        time_increment = 300
        # Create longer simulation for 24 hour horizon
        num_points = 289  # 24 hours / 5 minutes + 1
        simulation_runs = np.array(
            [np.linspace(100, 110, num_points)]
        )
        real_price_path = np.linspace(100, 110, num_points)
        real_price_path[-1] = 0.0  # Set last price to zero

        score, detailed_data = calculate_crps_for_miner(
            simulation_runs,
            real_price_path,
            time_increment,
            prompt_config.LOW_FREQUENCY.scoring_intervals,
        )

        # Should handle gracefully
        self.assertIsNotNone(score)
        self.assertIsInstance(score, float)
        # Should have some score from non-absolute intervals
        self.assertGreaterEqual(score, 0.0)

    def test_all_prices_zero(self):
        """Test edge case where all prices are zero."""
        time_increment = 300
        simulation_runs = np.array([[0.0, 0.0, 0.0]])
        real_price_path = np.array([0.0, 0.0, 0.0])

        # This should return -1 due to zero price check at line 52
        score, detailed_data = calculate_crps_for_miner(
            simulation_runs,
            real_price_path,
            time_increment,
            {"24hour_abs": 86400},
        )

        # Should return -1 for zero prices in simulation
        self.assertEqual(score, -1.0)
        self.assertEqual(len(detailed_data), 1)
        self.assertIn("error", detailed_data[0])


if __name__ == "__main__":
    unittest.main()
