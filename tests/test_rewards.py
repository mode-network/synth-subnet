import unittest
from unittest.mock import Mock
from datetime import datetime

import numpy as np
from numpy.testing import assert_almost_equal, assert_equal

from simulation.validator.forward import remove_zero_rewards
from simulation.validator.miner_data_handler import MinerDataHandler
from simulation.simulation_input import SimulationInput
from simulation.validator.price_data_provider import PriceDataProvider
from simulation.validator.reward import get_rewards, compute_softmax
from tests.utils import generate_values


class TestRewards(unittest.TestCase):
    def setUp(self):
        """Set up a temporary file for testing."""
        self.price_data_provider = PriceDataProvider("BTC")

    def tearDown(self):
        pass

    def test_compute_softmax_1(self):
        score_values = np.array([1000, 1500, 2000])
        expected_score = np.array([0.506, 0.307, 0.186])

        actual_score = compute_softmax(score_values)

        assert_almost_equal(actual_score, expected_score, decimal=3)

    def test_compute_softmax_2(self):
        score_values = np.array([1000, 1500, 2000, -1])
        expected_score = np.array([0.506, 0.307, 0.186, 0])

        actual_score = compute_softmax(score_values)

        assert_almost_equal(actual_score, expected_score, decimal=3)

    def test_remove_zero_rewards(self):
        rewards = np.array([0.0, 5.0, 0.0, 10.0])
        miner_uids = [0, 1, 2, 3]

        filtered_rewards, filtered_miner_uids = remove_zero_rewards(rewards, miner_uids)

        assert_equal(filtered_rewards, np.array([5.0, 10.0]))
        self.assertEqual(len(filtered_miner_uids), 2)
        self.assertEqual(1, filtered_miner_uids[0])
        self.assertEqual(3, filtered_miner_uids[1])

    def test_get_rewards(self):
        miner_id = 0
        start_time = "2024-11-26T00:00:00"
        current_time = "2024-11-28T00:00:00"

        values = generate_values(datetime.fromisoformat(start_time))
        self.handler.set_values(miner_id, start_time, values)

        softmax_scores = get_rewards(
            self.handler,
            self.price_data_provider,
            SimulationInput(
                asset="BTC",
                start_time=current_time,
                time_increment=60,  # default: 5 mins
                time_length=3600,  # default: 1 day
                num_simulations=1  # default: 100
            ),
            [miner_id],  # TODO: add another test with more miners
            current_time
        )
        print(softmax_scores)
        # TODO: assert the scores

    def test_get_rewards_scores(self):
        mock_miner_data_handler = Mock()
        mock_price_data_provider = Mock()

        miner_uids = [1, 2, 3]
        validation_time = "2024-11-25T20:30:00"

        def mock_get_values(miner_uid, mock_validation_time):
            if miner_uid == 1:
                return [
                    [
                        {"time": "2024-11-25T20:20:00", "price": 90000},
                        {"time": "2024-11-25T20:25:00", "price": 91000},
                        {"time": "2024-11-25T20:30:00", "price": 92000},
                        {"time": "2024-11-25T20:35:00", "price": 92500},
                        {"time": "2024-11-25T20:40:00", "price": 92600},
                        {"time": "2024-11-25T20:45:00", "price": 92500}
                    ],
                    [
                        {"time": "2024-11-25T20:20:00", "price": 90500},
                        {"time": "2024-11-25T20:25:00", "price": 91500},
                        {"time": "2024-11-25T20:30:00", "price": 92500},
                        {"time": "2024-11-25T20:35:00", "price": 93500},
                        {"time": "2024-11-25T20:40:00", "price": 92900},
                        {"time": "2024-11-25T20:45:00", "price": 92100}
                    ],
                    [
                        {"time": "2024-11-25T20:20:00", "price": 91500},
                        {"time": "2024-11-25T20:25:00", "price": 92500},
                        {"time": "2024-11-25T20:30:00", "price": 94500},
                        {"time": "2024-11-25T20:35:00", "price": 90500},
                        {"time": "2024-11-25T20:40:00", "price": 90900},
                        {"time": "2024-11-25T20:45:00", "price": 90100}
                    ]
                ]
            elif miner_uid == 2:
                return [
                    [
                        {"time": "2024-11-25T20:20:00", "price": 100000},
                        {"time": "2024-11-25T20:25:00", "price": 101000},
                        {"time": "2024-11-25T20:30:00", "price": 102000},
                        {"time": "2024-11-25T20:35:00", "price": 102500},
                        {"time": "2024-11-25T20:40:00", "price": 102600},
                        {"time": "2024-11-25T20:45:00", "price": 102500}
                    ],
                    [
                        {"time": "2024-11-25T20:20:00", "price": 100500},
                        {"time": "2024-11-25T20:25:00", "price": 101500},
                        {"time": "2024-11-25T20:30:00", "price": 102500},
                        {"time": "2024-11-25T20:35:00", "price": 103500},
                        {"time": "2024-11-25T20:40:00", "price": 102900},
                        {"time": "2024-11-25T20:45:00", "price": 102100}
                    ],
                    [
                        {"time": "2024-11-25T20:20:00", "price": 101500},
                        {"time": "2024-11-25T20:25:00", "price": 102500},
                        {"time": "2024-11-25T20:30:00", "price": 104500},
                        {"time": "2024-11-25T20:35:00", "price": 100500},
                        {"time": "2024-11-25T20:40:00", "price": 100900},
                        {"time": "2024-11-25T20:45:00", "price": 100100}
                    ]
                ]
            elif miner_uid == 3:
                return [
                    [
                        {"time": "2024-11-25T20:20:00", "price": 50000},
                        {"time": "2024-11-25T20:25:00", "price": 51000},
                        {"time": "2024-11-25T20:30:00", "price": 52000},
                        {"time": "2024-11-25T20:35:00", "price": 52500},
                        {"time": "2024-11-25T20:40:00", "price": 52600},
                        {"time": "2024-11-25T20:45:00", "price": 52500}
                    ],
                    [
                        {"time": "2024-11-25T20:20:00", "price": 60000},
                        {"time": "2024-11-25T20:25:00", "price": 61000},
                        {"time": "2024-11-25T20:30:00", "price": 62000},
                        {"time": "2024-11-25T20:35:00", "price": 62500},
                        {"time": "2024-11-25T20:40:00", "price": 62600},
                        {"time": "2024-11-25T20:45:00", "price": 62500}
                    ],
                    [
                        {"time": "2024-11-25T20:20:00", "price": 70000},
                        {"time": "2024-11-25T20:25:00", "price": 71000},
                        {"time": "2024-11-25T20:30:00", "price": 72000},
                        {"time": "2024-11-25T20:35:00", "price": 72500},
                        {"time": "2024-11-25T20:40:00", "price": 72600},
                        {"time": "2024-11-25T20:45:00", "price": 72500}
                    ]
                ]

        mock_miner_data_handler.get_values.side_effect = mock_get_values

        mock_price_data_provider.fetch_data.return_value = [
            {"time": "2024-11-25T20:00:00", "price": 90000},
            {"time": "2024-11-25T20:05:00", "price": 91000},
            {"time": "2024-11-25T20:10:00", "price": 92000},
            {"time": "2024-11-25T20:15:00", "price": 92500},
            {"time": "2024-11-25T20:20:00", "price": 92600},
            {"time": "2024-11-25T20:25:00", "price": 92500},
            {"time": "2024-11-25T20:30:00", "price": 93500}
        ]

        simulation_input = SimulationInput(
            asset="BTC",
            start_time=validation_time,
            time_increment=300,  # 5 mins
            time_length=86400,  # 1 day
            num_simulations=3
        )

        result = get_rewards(
            miner_data_handler=mock_miner_data_handler,
            price_data_provider=mock_price_data_provider,
            simulation_input=simulation_input,
            miner_uids=miner_uids,
            validation_time=validation_time,
        )

        print(result)

    def test_get_rewards_scores_if_one_of_the_miners_returns_no_data(self):
        mock_miner_data_handler = Mock()
        mock_price_data_provider = Mock()

        miner_uids = [1, 2, 3]
        validation_time = "2024-11-25T20:30:00"

        def mock_get_values(miner_uid, mock_validation_time):
            if miner_uid == 1:
                return [
                    {"time": "2024-11-25T20:20:00", "price": 90000},
                    {"time": "2024-11-25T20:25:00", "price": 91000},
                    {"time": "2024-11-25T20:30:00", "price": 92000},
                    {"time": "2024-11-25T20:35:00", "price": 92500},
                    {"time": "2024-11-25T20:40:00", "price": 92600},
                    {"time": "2024-11-25T20:45:00", "price": 92500}
                ]
            elif miner_uid == 2:
                return []
            elif miner_uid == 3:
                return [
                    {"time": "2024-11-25T20:20:00", "price": 50000},
                    {"time": "2024-11-25T20:25:00", "price": 51000},
                    {"time": "2024-11-25T20:30:00", "price": 52000},
                    {"time": "2024-11-25T20:35:00", "price": 52500},
                    {"time": "2024-11-25T20:40:00", "price": 52600},
                    {"time": "2024-11-25T20:45:00", "price": 52500}
                ]

        mock_miner_data_handler.get_values.side_effect = mock_get_values

        mock_price_data_provider.fetch_data.return_value = [
            {"time": "2024-11-25T20:00:00", "price": 90000},
            {"time": "2024-11-25T20:05:00", "price": 91000},
            {"time": "2024-11-25T20:10:00", "price": 92000},
            {"time": "2024-11-25T20:15:00", "price": 92500},
            {"time": "2024-11-25T20:20:00", "price": 92600},
            {"time": "2024-11-25T20:25:00", "price": 92500},
            {"time": "2024-11-25T20:30:00", "price": 93500}
        ]

        simulation_input = SimulationInput(
            asset="BTC",
            start_time=validation_time,
            time_increment=300,  # default: 5 mins
            time_length=86400,  # default: 1 day
            num_simulations=1  # default: 100
        )

        result = get_rewards(
            miner_data_handler=mock_miner_data_handler,
            price_data_provider=mock_price_data_provider,
            simulation_input=simulation_input,
            miner_uids=miner_uids,
            start_time=validation_time,
        )

        print(result)
