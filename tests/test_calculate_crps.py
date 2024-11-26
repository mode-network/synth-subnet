import unittest

import numpy as np

from simulation.validator.crps_calculation import calculate_crps_for_miner


class TestCalculateCrps(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_calculate_crps_for_miner_1(self):
        miner_uid = 1
        time_increment = 300  # 300 seconds = 5 minutes
        predictions_path = [90000, 91000, 92000]
        real_price_path = [92600, 92500, 93500]

        score = calculate_crps_for_miner(
            miner_uid,
            np.array([predictions_path]),
            np.array(real_price_path),
            time_increment
        )

        self.assertEqual(score, 1100)

    def test_calculate_crps_for_miner_2(self):
        miner_uid = 1
        time_increment = 300  # 300 seconds = 5 minutes
        predictions_path = [90000, 91000, 92000, 92500, 92600]
        real_price_path = [92600, 92500, 92600, 92500, 93500]

        score = calculate_crps_for_miner(
            miner_uid,
            np.array([predictions_path]),
            np.array(real_price_path),
            time_increment
        )

        self.assertEqual(score, 3500)

    def test_calculate_crps_for_miner_3(self):
        miner_uid = 1
        time_increment = 300  # 300 seconds = 5 minutes
        predictions_path = [50000, 51000, 52000]
        real_price_path = [92600, 92500, 93500]

        score = calculate_crps_for_miner(
            miner_uid,
            np.array([predictions_path]),
            np.array(real_price_path),
            time_increment
        )

        self.assertEqual(score, 1100)
