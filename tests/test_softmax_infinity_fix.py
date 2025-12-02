"""
Test suite for the softmax infinity handling bug fix.

This test validates that the fix for softmax numerical instability
with infinite scores works correctly.
"""

import unittest
import numpy as np

from synth.validator.reward import compute_softmax


class TestSoftmaxInfinityFix(unittest.TestCase):
    """Test cases for the softmax infinity bug fix."""

    def test_softmax_with_single_infinity(self):
        """Test softmax handles a single infinite score correctly."""
        scores = np.array([100.0, 200.0, float("inf"), 150.0])
        beta = -0.1

        weights = compute_softmax(scores, beta)

        # Should not be NaN
        self.assertFalse(np.any(np.isnan(weights)))
        # Should sum to 1
        self.assertAlmostEqual(np.sum(weights), 1.0, places=10)
        # Infinite score should get ~0 weight
        self.assertLess(weights[2], 0.001)
        # Other scores should have positive weights
        self.assertGreater(weights[0], 0.0)
        self.assertGreater(weights[1], 0.0)
        self.assertGreater(weights[3], 0.0)

    def test_softmax_with_multiple_infinities(self):
        """Test softmax handles multiple infinite scores correctly."""
        scores = np.array([100.0, float("inf"), 150.0, float("inf")])
        beta = -0.1

        weights = compute_softmax(scores, beta)

        # Should not be NaN
        self.assertFalse(np.any(np.isnan(weights)))
        # Should sum to 1
        self.assertAlmostEqual(np.sum(weights), 1.0, places=10)
        # Infinite scores should get ~0 weight
        self.assertLess(weights[1], 0.001)
        self.assertLess(weights[3], 0.001)
        # Finite scores should share most of the weight
        # Note: 100 < 150, so with negative beta, 100 gets much more weight
        self.assertGreater(weights[0], 0.9)  # Best score gets most weight
        self.assertGreater(weights[2], 0.001)  # Worse finite score gets some weight

    def test_softmax_all_infinite(self):
        """Test softmax when all scores are infinite."""
        scores = np.array([float("inf"), float("inf"), float("inf")])
        beta = -0.1

        weights = compute_softmax(scores, beta)

        # Should not be NaN
        self.assertFalse(np.any(np.isnan(weights)))
        # Should return equal weights
        expected_weight = 1.0 / 3.0
        for weight in weights:
            self.assertAlmostEqual(weight, expected_weight, places=10)

    def test_softmax_empty_array(self):
        """Test softmax handles empty array gracefully."""
        scores = np.array([])
        beta = -0.1

        weights = compute_softmax(scores, beta)

        # Should return empty array
        self.assertEqual(len(weights), 0)

    def test_softmax_single_score(self):
        """Test softmax with single score."""
        scores = np.array([100.0])
        beta = -0.1

        weights = compute_softmax(scores, beta)

        # Should return weight of 1.0
        self.assertEqual(len(weights), 1)
        self.assertAlmostEqual(weights[0], 1.0, places=10)

    def test_softmax_all_equal_scores(self):
        """Test softmax when all scores are equal."""
        scores = np.array([100.0, 100.0, 100.0, 100.0])
        beta = -0.1

        weights = compute_softmax(scores, beta)

        # Should return equal weights
        expected_weight = 1.0 / 4.0
        for weight in weights:
            self.assertAlmostEqual(weight, expected_weight, places=10)

    def test_softmax_negative_infinity(self):
        """Test softmax with negative infinity (best possible score)."""
        scores = np.array([100.0, 200.0, float("-inf"), 150.0])
        beta = -0.1

        weights = compute_softmax(scores, beta)

        # Should not be NaN
        self.assertFalse(np.any(np.isnan(weights)))
        # Should sum to 1
        self.assertAlmostEqual(np.sum(weights), 1.0, places=10)
        # Negative infinity with negative beta should get most weight
        # exp(-0.1 * -inf) = exp(+inf) = inf → should get replaced and handled
        self.assertGreater(weights[2], 0.0)

    def test_softmax_very_large_finite_scores(self):
        """Test softmax with very large but finite scores."""
        scores = np.array([1e10, 1e11, 1e12, 1e10])
        beta = -0.1

        weights = compute_softmax(scores, beta)

        # Should not be NaN or inf
        self.assertFalse(np.any(np.isnan(weights)))
        self.assertFalse(np.any(np.isinf(weights)))
        # Should sum to 1
        self.assertAlmostEqual(np.sum(weights), 1.0, places=10)
        # Largest score (worst) should get smallest weight
        self.assertLess(weights[2], weights[0])

    def test_softmax_mixed_inf_and_large_finite(self):
        """Test softmax with both infinity and large finite scores."""
        scores = np.array([100.0, 1e10, float("inf"), 1e5])
        beta = -0.1

        weights = compute_softmax(scores, beta)

        # Should not be NaN
        self.assertFalse(np.any(np.isnan(weights)))
        # Should sum to 1
        self.assertAlmostEqual(np.sum(weights), 1.0, places=10)
        # Infinite score should get ~0 weight
        self.assertLess(weights[2], 0.001)

    def test_softmax_positive_beta(self):
        """Test softmax with positive beta (unusual but should work)."""
        scores = np.array([100.0, 200.0, 150.0])
        beta = 0.1  # Positive beta

        weights = compute_softmax(scores, beta)

        # Should not be NaN
        self.assertFalse(np.any(np.isnan(weights)))
        # Should sum to 1
        self.assertAlmostEqual(np.sum(weights), 1.0, places=10)
        # With positive beta, higher scores get more weight
        self.assertGreater(weights[1], weights[0])

    def test_softmax_zero_beta(self):
        """Test softmax with beta = 0 (should give equal weights)."""
        scores = np.array([100.0, 200.0, 150.0, 300.0])
        beta = 0.0

        weights = compute_softmax(scores, beta)

        # Should return equal weights (exp(0) = 1 for all)
        expected_weight = 1.0 / 4.0
        for weight in weights:
            self.assertAlmostEqual(weight, expected_weight, places=10)

    def test_softmax_numerical_stability(self):
        """Test that softmax is numerically stable with large score ranges."""
        scores = np.array([1.0, 1000.0, 10000.0, 100000.0])
        beta = -0.1

        weights = compute_softmax(scores, beta)

        # Should not be NaN or inf
        self.assertFalse(np.any(np.isnan(weights)))
        self.assertFalse(np.any(np.isinf(weights)))
        # Should sum to 1
        self.assertAlmostEqual(np.sum(weights), 1.0, places=10)
        # All weights should be positive
        self.assertTrue(np.all(weights > 0))

    def test_softmax_with_actual_rolling_averages(self):
        """Test softmax with realistic rolling average scores."""
        # Simulate realistic rolling averages (CRPS scores)
        scores = np.array([150.5, 200.3, float("inf"), 175.8, 220.1])
        beta = -0.1

        weights = compute_softmax(scores, beta)

        # Should not be NaN
        self.assertFalse(np.any(np.isnan(weights)))
        # Should sum to 1
        self.assertAlmostEqual(np.sum(weights), 1.0, places=10)
        # Miner with inf should get ~0 weight
        self.assertLess(weights[2], 0.001)
        # Best miner (lowest score) should get highest weight
        best_idx = np.argmin(scores[np.isfinite(scores)])
        self.assertEqual(best_idx, 0)  # Score 150.5
        self.assertGreater(weights[0], weights[1])
        self.assertGreater(weights[0], weights[3])
        self.assertGreater(weights[0], weights[4])

    def test_softmax_preserves_order(self):
        """Test that softmax preserves score ordering (lower score = higher weight)."""
        scores = np.array([100.0, 200.0, 150.0, 250.0, 50.0])
        beta = -0.1

        weights = compute_softmax(scores, beta)

        # Best score (50.0) should have highest weight
        best_idx = np.argmin(scores)
        self.assertEqual(best_idx, 4)
        self.assertEqual(weights[4], np.max(weights))

        # Worst score (250.0) should have lowest weight
        worst_idx = np.argmax(scores)
        self.assertEqual(worst_idx, 3)
        self.assertEqual(weights[3], np.min(weights))


if __name__ == "__main__":
    unittest.main()
