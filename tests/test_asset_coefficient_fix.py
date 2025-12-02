"""
Test suite for the asset coefficient normalization bug fix.

This test validates that the fix for the division by zero and incorrect
normalization bug in apply_per_asset_coefficients works correctly.
"""

import unittest
import pandas as pd
import numpy as np

from synth.validator.moving_average import apply_per_asset_coefficients


class TestAssetCoefficientFix(unittest.TestCase):
    """Test cases for the asset coefficient bug fix."""

    def test_empty_dataframe_no_crash(self):
        """Test that empty dataframe doesn't cause division by zero."""
        df_empty = pd.DataFrame({"asset": [], "prompt_score_v3": []})

        # Should not crash
        weighted_scores, weights = apply_per_asset_coefficients(df_empty)

        self.assertEqual(len(weighted_scores), 0)
        self.assertEqual(len(weights), 0)

    def test_single_asset_weighting(self):
        """Test that single asset gets correct weight applied."""
        df = pd.DataFrame({"asset": ["BTC"], "prompt_score_v3": [100.0]})

        weighted_scores, weights = apply_per_asset_coefficients(df)

        # BTC coefficient is 1.0
        self.assertEqual(weighted_scores.iloc[0], 100.0)
        self.assertEqual(weights.iloc[0], 1.0)

    def test_multiple_assets_independent_weighting(self):
        """Test that each asset gets weighted independently."""
        df = pd.DataFrame(
            {
                "asset": ["BTC", "ETH", "SOL"],
                "prompt_score_v3": [100.0, 100.0, 100.0],
            }
        )

        weighted_scores, weights = apply_per_asset_coefficients(df)

        # Check that BTC score is weighted by 1.0
        btc_idx = df[df["asset"] == "BTC"].index[0]
        self.assertAlmostEqual(weighted_scores.iloc[btc_idx], 100.0, places=5)
        self.assertAlmostEqual(weights.iloc[btc_idx], 1.0, places=5)

        # Check that ETH score is weighted by 0.621...
        eth_idx = df[df["asset"] == "ETH"].index[0]
        self.assertAlmostEqual(
            weighted_scores.iloc[eth_idx], 62.10893136676585, places=5
        )
        self.assertAlmostEqual(
            weights.iloc[eth_idx], 0.6210893136676585, places=5
        )

        # Check that SOL score is weighted by 0.502...
        sol_idx = df[df["asset"] == "SOL"].index[0]
        self.assertAlmostEqual(
            weighted_scores.iloc[sol_idx], 50.21491038021751, places=5
        )
        self.assertAlmostEqual(
            weights.iloc[sol_idx], 0.5021491038021751, places=5
        )

    def test_consistency_across_different_contexts(self):
        """
        Test that BTC score gets same weight regardless of other assets present.
        This is the key bug fix - weighting should be consistent.
        """
        # Context 1: Only BTC
        df1 = pd.DataFrame({"asset": ["BTC"], "prompt_score_v3": [100.0]})

        # Context 2: BTC with other assets
        df2 = pd.DataFrame(
            {
                "asset": ["BTC", "ETH", "SOL", "XAU"],
                "prompt_score_v3": [100.0, 200.0, 300.0, 400.0],
            }
        )

        weighted_scores1, weights1 = apply_per_asset_coefficients(df1)
        weighted_scores2, weights2 = apply_per_asset_coefficients(df2)

        # BTC weighted score should be the same in both contexts
        btc_weighted_score1 = weighted_scores1.iloc[0]
        btc_weighted_score2 = weighted_scores2.iloc[0]

        self.assertAlmostEqual(
            btc_weighted_score1, btc_weighted_score2, places=10
        )
        self.assertAlmostEqual(btc_weighted_score1, 100.0, places=10)

    def test_weighted_average_calculation(self):
        """Test that weighted average is calculated correctly."""
        df = pd.DataFrame(
            {
                "asset": ["BTC", "BTC", "ETH", "ETH"],
                "prompt_score_v3": [100.0, 200.0, 100.0, 200.0],
            }
        )

        weighted_scores, weights = apply_per_asset_coefficients(df)

        # Calculate weighted average manually
        total_weighted_score = weighted_scores.sum()
        total_weight = weights.sum()
        weighted_avg = total_weighted_score / total_weight

        # BTC: (100*1.0 + 200*1.0) = 300
        # ETH: (100*0.621 + 200*0.621) = 186.33
        # Total weighted: 486.33
        # Total weight: 1.0 + 1.0 + 0.621 + 0.621 = 3.242
        # Weighted avg: 486.33 / 3.242 = 150.0

        expected_total_weighted = 100.0 + 200.0 + 62.10893136676585 + 124.2178627335317
        expected_total_weight = 1.0 + 1.0 + 0.6210893136676585 + 0.6210893136676585
        expected_avg = expected_total_weighted / expected_total_weight

        self.assertAlmostEqual(weighted_avg, expected_avg, places=5)

    def test_unknown_asset_defaults_to_one(self):
        """Test that unknown assets get default weight of 1.0."""
        df = pd.DataFrame(
            {"asset": ["UNKNOWN_ASSET"], "prompt_score_v3": [100.0]}
        )

        weighted_scores, weights = apply_per_asset_coefficients(df)

        # Unknown asset should default to weight 1.0
        self.assertEqual(weighted_scores.iloc[0], 100.0)
        self.assertEqual(weights.iloc[0], 1.0)

    def test_all_known_assets(self):
        """Test that all known assets have correct coefficients."""
        df = pd.DataFrame(
            {
                "asset": ["BTC", "ETH", "XAU", "SOL"],
                "prompt_score_v3": [1.0, 1.0, 1.0, 1.0],
            }
        )

        weighted_scores, weights = apply_per_asset_coefficients(df)

        expected_weights = {
            "BTC": 1.0,
            "ETH": 0.6210893136676585,
            "XAU": 1.4550630831254674,
            "SOL": 0.5021491038021751,
        }

        for i, asset in enumerate(df["asset"]):
            self.assertAlmostEqual(
                weights.iloc[i], expected_weights[asset], places=10
            )
            self.assertAlmostEqual(
                weighted_scores.iloc[i], expected_weights[asset], places=10
            )

    def test_zero_scores_handled_correctly(self):
        """Test that zero scores are handled without issues."""
        df = pd.DataFrame(
            {"asset": ["BTC", "ETH"], "prompt_score_v3": [0.0, 0.0]}
        )

        weighted_scores, weights = apply_per_asset_coefficients(df)

        # Zero scores should remain zero after weighting
        self.assertEqual(weighted_scores.iloc[0], 0.0)
        self.assertEqual(weighted_scores.iloc[1], 0.0)

        # But weights should still be correct
        self.assertEqual(weights.iloc[0], 1.0)
        self.assertAlmostEqual(weights.iloc[1], 0.6210893136676585, places=10)

    def test_negative_scores_handled_correctly(self):
        """Test that negative scores (if they occur) are handled correctly."""
        df = pd.DataFrame(
            {"asset": ["BTC", "ETH"], "prompt_score_v3": [-100.0, -50.0]}
        )

        weighted_scores, weights = apply_per_asset_coefficients(df)

        # Negative scores should be weighted correctly
        self.assertEqual(weighted_scores.iloc[0], -100.0)
        self.assertAlmostEqual(
            weighted_scores.iloc[1], -31.054465683382925, places=5
        )

    def test_large_dataframe_performance(self):
        """Test that the function handles large dataframes efficiently."""
        # Create a large dataframe with 10000 rows
        n_rows = 10000
        df = pd.DataFrame(
            {
                "asset": np.random.choice(
                    ["BTC", "ETH", "SOL", "XAU"], n_rows
                ),
                "prompt_score_v3": np.random.uniform(0, 1000, n_rows),
            }
        )

        # Should complete without issues
        weighted_scores, weights = apply_per_asset_coefficients(df)

        self.assertEqual(len(weighted_scores), n_rows)
        self.assertEqual(len(weights), n_rows)

        # Verify all weights are positive
        self.assertTrue((weights > 0).all())


if __name__ == "__main__":
    unittest.main()
