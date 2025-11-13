import unittest

import numpy as np

from synth.validator.crps_calculation import (
    calculate_crps_for_miner,
    label_observed_blocks,
)
from synth.validator.reward import compute_softmax


class TestCalculateCrps(unittest.TestCase):
    def test_calculate_crps_for_miner_1(self):
        time_increment = 300  # 300 seconds = 5 minutes
        predictions_path = [[90000, 91000, 92000], [90000, 91000, 92000]]
        real_price_path = [92600, 92500, 93500]

        sum_all_scores, _ = calculate_crps_for_miner(
            np.array(predictions_path),
            np.array(real_price_path),
            time_increment,
        )
        self.assertEqual(sum_all_scores, 284.1200564488584)

    def test_calculate_crps_for_miner_1_b(self):
        time_increment = 300  # 300 seconds = 5 minutes
        predictions_path = [[900, 910, 920], [900, 910, 920]]
        real_price_path = [926, 925, 935]

        sum_all_scores, _ = calculate_crps_for_miner(
            np.array(predictions_path),
            np.array(real_price_path),
            time_increment,
        )
        self.assertEqual(sum_all_scores, 284.1200564488584)

    def test_calculate_crps_for_miner_zero(self):
        time_increment = 300  # 300 seconds = 5 minutes
        predictions_path = [[50, 60, 70]]
        real_price_path = [50, 60, 70]

        sum_all_scores, _ = calculate_crps_for_miner(
            np.array(predictions_path),
            np.array(real_price_path),
            time_increment,
        )
        self.assertEqual(sum_all_scores, 0)

    def test_calculate_crps_for_miner_2(self):
        time_increment = 300  # 300 seconds = 5 minutes
        predictions_path = [90000, 91000, 92000, 92500, 92600]
        real_price_path = [92600, 92500, 92600, 92500, 93500]

        sum_all_scores, _ = calculate_crps_for_miner(
            np.array([predictions_path]),
            np.array(real_price_path),
            time_increment,
        )

        self.assertEqual(sum_all_scores, 479.6904902048716)

    def test_calculate_crps_for_miner_3(self):
        time_increment = 300  # 300 seconds = 5 minutes
        predictions_path = [50000, 51000, 52000]
        real_price_path = [92600, 92500, 93500]

        sum_all_scores, _ = calculate_crps_for_miner(
            np.array([predictions_path]),
            np.array(real_price_path),
            time_increment,
        )

        self.assertEqual(sum_all_scores, 4737.272133130346)

    def test_calculate_crps_for_miner_4(self):
        """
        Showcases crps calculation for a miner.
        In the real case scenario you are going to have 289 time points,
        this is a simplified test that takes only 3 time points.

        The idea is, say we have the following predictions from a miner:
        miner_prediction = [
            [
                {"time": "2025-01-30T17:33:00+00:00", "price": 50000},
                {"time": "2025-01-30T17:38:00+00:00", "price": 51000},
                {"time": "2025-01-30T17:43:00+00:00", "price": 52000}
            ],
            [
                {"time": "2025-01-30T17:33:00+00:00", "price": 60000},
                {"time": "2025-01-30T17:38:00+00:00", "price": 70000},
                {"time": "2025-01-30T17:43:00+00:00", "price": 80000}
            ],
            [
                {"time": "2025-01-30T17:33:00+00:00", "price": 90000},
                {"time": "2025-01-30T17:38:00+00:00", "price": 70000},
                {"time": "2025-01-30T17:43:00+00:00", "price": 50000}
            ]
        ]

        and the corresponding real prices at the same time points:
        [
            {"time": "2025-01-30T17:33:00+00:00", "price": 105165.69445825},
            {"time": "2025-01-30T17:38:00+00:00", "price": 105016.21888945},
            {"time": "2025-01-30T17:43:00+00:00", "price": 105066.94377502}
        ]

        we remove the datetime and leave only the prices,
        and send them to crps function
        """
        time_increment = 300  # 300 seconds = 5 minutes
        predictions_path = [
            [50000, 51000, 52000],
            [10000, 70000, 50000],
            [90000, 70000, 50000],
        ]
        real_price_path = [105165.69445825, 105016.21888945, 105066.94377502]

        sum_all_scores, _ = calculate_crps_for_miner(
            np.array(predictions_path),
            np.array(real_price_path),
            time_increment,
        )

        self.assertEqual(sum_all_scores, 13413.599141058676)

    def test_calculate_crps_for_miner_5(self):
        """
        Test crps calculation with gaps inside the real price array.
        """
        time_increment = 300  # 300 seconds = 5 minutes
        predictions_path = [50, 60, 70, 80, 90, 100, 110, 120, 130]
        real_price_path = [50, 60, np.nan, 80, 90, np.nan, np.nan, 120, 130]

        sum_all_scores, _ = calculate_crps_for_miner(
            np.array([predictions_path]),
            np.array(real_price_path),
            time_increment,
        )

        self.assertEqual(sum_all_scores, 0.0)

    def test_calculate_crps_for_miner_6(self):
        """
        Test crps calculation with gaps in the real price array.
        """
        time_increment = 300  # 300 seconds = 5 minutes
        predictions_path = [50, 60, 70, 80, 90, 100, 110, 120, 130]
        real_price_path = [50, 60, np.nan, 80, 90, np.nan, np.nan, 120, 130]

        sum_all_scores, _ = calculate_crps_for_miner(
            np.array([predictions_path]),
            np.array(real_price_path),
            time_increment,
        )

        self.assertEqual(sum_all_scores, 0.0)

    def test_calculate_crps_for_miner_7(self):
        """
        Test crps calculation with gaps inside and in the extremes of the real price array.
        """
        time_increment = 300  # 300 seconds = 5 minutes
        predictions_path = [50, 60, 70, 80, 90, 100, 110, 120, 130]
        real_price_path = [np.nan, 60, 70, np.nan, 90, 100, 110, 120, np.nan]

        sum_all_scores, _ = calculate_crps_for_miner(
            np.array([predictions_path]),
            np.array(real_price_path),
            time_increment,
        )

        self.assertEqual(sum_all_scores, 0.0)

    def test_calculate_crps_for_miner_8(self):
        """
        Assess that the crps is 0 with fully unobserved price array.
        """
        time_increment = 300  # 300 seconds = 5 minutes
        predictions_path = [50, 60, 70, 80, 90, 100, 110, 120, 130]
        real_price_path = [
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            np.nan,
        ]

        sum_all_scores, _ = calculate_crps_for_miner(
            np.array([predictions_path]),
            np.array(real_price_path),
            time_increment,
        )

        self.assertEqual(sum_all_scores, 0.0)

    def test_calculate_crps_for_miner_9(self):
        """
        Test crps calculation with gaps in the real price array.
        Assess that it is different than the fully observed price array.
        """
        time_increment = 300  # 300 seconds = 5 minutes
        predictions_path = [55, 64, 70, 82.5, 89.2, 100, 110, 123.5, 131.2]
        real_price_path = [50, 60, np.nan, 80, 90, np.nan, np.nan, 120, 130]
        real_price_path_full = [50, 60, 70, 80, 90, 100, 110, 120, 130]

        sum_all_scores, _ = calculate_crps_for_miner(
            np.array([predictions_path]),
            np.array(real_price_path),
            time_increment,
        )

        sum_all_scores_2, _ = calculate_crps_for_miner(
            np.array([predictions_path]),
            np.array(real_price_path_full),
            time_increment,
        )

        with self.subTest("Check sum_all_scores equals expected"):
            self.assertEqual(sum_all_scores, 1103.6743957796587)

        with self.subTest(
            "Check sum_all_scores is less than sum_all_scores_2"
        ):
            self.assertLess(sum_all_scores, sum_all_scores_2)

    def test_calculate_crps_for_miner_10(self):
        time_increment = 300  # 300 seconds = 5 minutes
        predictions_path = [50, 60, np.nan, 80, 90, 92, 98, 120, 130]
        real_price_path = [55, 64, 70, 82.5, 89.2, 100, 110, 123.5, 131.2]

        sum_all_scores, _ = calculate_crps_for_miner(
            np.array([predictions_path]),
            np.array(real_price_path),
            time_increment,
        )

        self.assertEqual(np.isnan(sum_all_scores), True)

    def test_calculate_crps_for_miner_11(self):
        time_increment = 300  # 300 seconds = 5 minutes
        real_price_path = [50, 60, np.nan, 80, 90, np.nan, np.nan, 120, 130]
        predictions_path = [55, 64, 70, 82.5, 89.2, 100, 110, 123, 131]

        sum_all_scores, _ = calculate_crps_for_miner(
            np.array([predictions_path]),
            np.array(real_price_path),
            time_increment,
        )

        self.assertEqual(sum_all_scores, 1061.3650577065207)

    def test_calculate_crps_for_miner_12(self):
        time_increment = 300  # 300 seconds = 5 minutes
        real_price_path = [50, 60, np.nan, 80, 90, np.nan, np.nan, 120, 130]
        predictions_path = [
            1303196869523179600000000000000000000000000000000000000000000000000000000000000000000000000,
            0.00011997788254371478,
            70,
            82.5,
            89.2,
            100,
            110,
            123,
            131,
        ]

        sum_all_scores, _ = calculate_crps_for_miner(
            np.array([predictions_path]).astype(float),
            np.array(real_price_path),
            time_increment,
        )

        self.assertEqual(sum_all_scores, 12697.728694070156)

    def test_calculate_crps_for_miner_13(self):
        time_increment = 300  # 300 seconds = 5 minutes
        real_price_path = [50, 60, np.nan, 80, 90, np.nan, np.nan, 120, 130]
        predictions_path = [
            0.00011997788254371478,
            0,
            70,
            82.5,
            89.2,
            100,
            110,
            123,
            131,
        ]

        sum_all_scores, _ = calculate_crps_for_miner(
            np.array([predictions_path]).astype(float),
            np.array(real_price_path),
            time_increment,
        )

        self.assertEqual(sum_all_scores, -1)

    def test_normalization(self):
        result = compute_softmax(np.array([]), beta=-0.002)

        self.assertEqual(result.tolist(), [])

    def test_label_observed_blocks_all_observed(self):
        arr = np.array([1.0, 2.0, 3.0, 4.0])
        result = label_observed_blocks(arr)
        np.testing.assert_array_equal(result, [0, 0, 0, 0])

    def test_label_observed_blocks_all_nan(self):
        arr = np.array([np.nan, np.nan, np.nan])
        result = label_observed_blocks(arr)
        np.testing.assert_array_equal(result, [-1, -1, -1])

    def test_label_observed_blocks_single_block_with_nans(self):
        arr = np.array([1.0, 2.0, np.nan, 4.0, np.nan, np.nan, 7.0, 8.0])
        result = label_observed_blocks(arr)
        np.testing.assert_array_equal(result, [0, 0, -1, 1, -1, -1, 2, 2])

    def test_label_observed_blocks_nan_at_start(self):
        arr = np.array([np.nan, 1.0, 2.0, np.nan, 3.0])
        result = label_observed_blocks(arr)
        np.testing.assert_array_equal(result, [-1, 0, 0, -1, 1])

    def test_label_observed_blocks_nan_at_end(self):
        arr = np.array([1.0, 2.0, np.nan, np.nan])
        result = label_observed_blocks(arr)
        np.testing.assert_array_equal(result, [0, 0, -1, -1])

    def test_label_observed_blocks_alternating_nan(self):
        arr = np.array([1.0, np.nan, 2.0, np.nan, 3.0])
        result = label_observed_blocks(arr)
        np.testing.assert_array_equal(result, [0, -1, 1, -1, 2])

    def test_label_observed_blocks_single_value_observed(self):
        arr = np.array([np.nan, np.nan, 5.0, np.nan])
        result = label_observed_blocks(arr)
        np.testing.assert_array_equal(result, [-1, -1, 0, -1])

    def test_label_observed_blocks_empty_array(self):
        arr = np.array([])
        result = label_observed_blocks(arr)
        np.testing.assert_array_equal(result, [])
