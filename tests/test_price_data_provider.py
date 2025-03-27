import unittest
from unittest.mock import patch

from synth.simulation_input import SimulationInput
from synth.validator.price_data_provider import PriceDataProvider


class TestPriceDataProvider(unittest.TestCase):
    def setUp(self):
        # self.dataProvider = PriceDataProvider("BTC")
        self.dataProvider = PriceDataProvider("BTC")

    def tearDown(self):
        pass

    def test_fetch_data_all_prices(self):
        # 1739974320 - 2025-02-20T14:12:00+00:00
        # 1739974380 - 2025-02-20T14:13:00+00:00
        # 1739974440 - 2025-02-20T14:14:00+00:00
        # 1739974500 - 2025-02-20T14:15:00+00:00
        # 1739974560 - 2025-02-20T14:16:00+00:00
        # 1739974620 - 2025-02-20T14:17:00+00:00
        # 1739974680 - 2025-02-20T14:18:00+00:00
        mock_response = {
            "t": [
                1739974320,
                1739974380,
                1739974440,
                1739974500,
                1739974560,
                1739974620,
                1739974680,
            ],
            "c": [
                100000.23,
                101000.55,
                99000.55,
                102000.55,
                103000.55,
                105000.55,
                108000.867,
            ],
        }

        with patch("requests.get") as mock_get:
            mock_get.return_value.json.return_value = mock_response

            result = self.dataProvider.fetch_data(
                SimulationInput(
                    start_time="2025-02-19T14:12:00+00:00",
                    time_length=86400,
                )
            )

            assert result == [
                {
                    "time": "2025-02-19T14:12:00+00:00",
                    "price": 100000.23,
                },
                {
                    "time": "2025-02-19T14:17:00+00:00",
                    "price": 105000.55,
                },
            ]

    def test_fetch_data_gap_1(self):
        # 1739974320 - 2025-02-20T14:12:00+00:00
        # gap        - 2025-02-20T14:13:00+00:00
        # gap        - 2025-02-20T14:14:00+00:00
        # gap        - 2025-02-20T14:15:00+00:00
        # gap        - 2025-02-20T14:16:00+00:00
        # 1739974620 - 2025-02-20T14:17:00+00:00
        # 1739974680 - 2025-02-20T14:18:00+00:00
        mock_response = {
            "t": [1739974320, 1739974620, 1739974680],
            "c": [100000.23, 105000.55, 108000.867],
        }

        with patch("requests.get") as mock_get:
            mock_get.return_value.json.return_value = mock_response

            result = self.dataProvider.fetch_data(
                SimulationInput(
                    start_time="2025-02-19T14:12:00+00:00",
                    time_length=86400,
                )
            )

            assert result == [
                {
                    "time": "2025-02-19T14:12:00+00:00",
                    "price": 100000.23,
                },
                {
                    "time": "2025-02-19T14:17:00+00:00",
                    "price": 105000.55,
                },
            ]

    def test_fetch_data_gap_2(self):
        # 1739974320 - 2025-02-20T14:12:00+00:00
        # gap        - 2025-02-20T14:13:00+00:00
        # gap        - 2025-02-20T14:14:00+00:00
        # gap        - 2025-02-20T14:15:00+00:00
        # gap        - 2025-02-20T14:16:00+00:00
        # gap        - 2025-02-20T14:17:00+00:00
        # 1739974680 - 2025-02-20T14:18:00+00:00
        mock_response = {
            "t": [1739974320, 1739974680],
            "c": [100000.23, 108000.867],
        }

        with patch("requests.get") as mock_get:
            mock_get.return_value.json.return_value = mock_response

            result = self.dataProvider.fetch_data(
                SimulationInput(
                    start_time="2025-02-19T14:12:00+00:00",
                    time_length=86400,
                )
            )

            assert result == [
                {
                    "time": "2025-02-19T14:12:00+00:00",
                    "price": 100000.23,
                }
            ]

    def test_fetch_data_gap_3(self):
        # 1739974320 - 2025-02-20T14:12:00+00:00
        # gap        - 2025-02-20T14:13:00+00:00
        # gap        - 2025-02-20T14:14:00+00:00
        # gap        - 2025-02-20T14:15:00+00:00
        # gap        - 2025-02-20T14:16:00+00:00
        # gap        - 2025-02-20T14:17:00+00:00
        # 1739974680 - 2025-02-20T14:18:00+00:00
        # 1739974740 - 2025-02-20T14:19:00+00:00
        # 1739974800 - 2025-02-20T14:20:00+00:00
        # 1739974860 - 2025-02-21T14:21:00+00:00
        # 1739974920 - 2025-02-21T14:22:00+00:00
        mock_response = {
            "t": [
                1739974320,
                1739974680,
                1739974740,
                1739974800,
                1739974860,
                1739974920,
            ],
            "c": [
                100000.23,
                108000.867,
                99000.23,
                97123.55,
                105123.345,
                107995.889,
            ],
        }

        with patch("requests.get") as mock_get:
            mock_get.return_value.json.return_value = mock_response

            result = self.dataProvider.fetch_data(
                SimulationInput(
                    start_time="2025-02-19T14:12:00+00:00",
                    time_length=86400,
                )
            )

            assert result == [
                {
                    "time": "2025-02-19T14:12:00+00:00",
                    "price": 100000.23,
                },
                {
                    "time": "2025-02-19T14:22:00+00:00",
                    "price": 107995.889,
                },
            ]

    def test_fetch_data_no_prices(self):
        mock_response = {
            "t": [],
            "c": [],
        }

        with patch("requests.get") as mock_get:
            mock_get.return_value.json.return_value = mock_response

            result = self.dataProvider.fetch_data(
                SimulationInput(
                    start_time="2025-02-19T14:12:00+00:00",
                    time_length=86400,
                )
            )

            assert result == []

    def test_fetch_data_gap_from_start(self):
        # gap        - 2025-02-20T14:12:00+00:00
        # gap        - 2025-02-20T14:13:00+00:00
        # gap        - 2025-02-20T14:14:00+00:00
        # gap        - 2025-02-20T14:15:00+00:00
        # gap        - 2025-02-20T14:16:00+00:00
        # gap        - 2025-02-20T14:17:00+00:00
        # 1739974680 - 2025-02-20T14:18:00+00:00
        # 1739974740 - 2025-02-20T14:19:00+00:00
        # 1739974800 - 2025-02-20T14:20:00+00:00
        # 1739974860 - 2025-02-20T14:21:00+00:00
        # 1739974920 - 2025-02-20T14:22:00+00:00
        mock_response = {
            "t": [1739974680, 1739974740, 1739974800, 1739974860, 1739974920],
            "c": [108000.867, 99000.23, 97123.55, 105123.345, 107995.889],
        }

        with patch("requests.get") as mock_get:
            mock_get.return_value.json.return_value = mock_response

            result = self.dataProvider.fetch_data(
                SimulationInput(
                    start_time="2025-02-19T14:12:00+00:00",
                    time_length=86400,
                )
            )

            assert result == [
                {
                    "time": "2025-02-19T14:22:00+00:00",
                    "price": 107995.889,
                }
            ]

    def test_fetch_data_gap_from_start_2(self):
        # gap        - 2025-02-20T14:12:00+00:00
        # 1739974380 - 2025-02-20T14:13:00+00:00
        # 1739974440 - 2025-02-20T14:14:00+00:00
        # 1739974500 - 2025-02-20T14:15:00+00:00
        # 1739974560 - 2025-02-20T14:16:00+00:00
        # 1739974620 - 2025-02-20T14:17:00+00:00
        # 1739974680 - 2025-02-20T14:18:00+00:00
        # 1739974740 - 2025-02-20T14:19:00+00:00
        # 1739974800 - 2025-02-20T14:20:00+00:00
        # 1739974860 - 2025-02-20T14:21:00+00:00
        # 1739974920 - 2025-02-20T14:22:00+00:00
        mock_response = {
            "t": [
                1739974380,
                1739974440,
                1739974500,
                1739974560,
                1739974620,
                1739974680,
                1739974740,
                1739974800,
                1739974860,
                1739974920,
            ],
            "c": [
                101000.55,
                99000.55,
                102000.55,
                103000.55,
                105000.55,
                108000.867,
                99000.23,
                97123.55,
                105123.345,
                107995.889,
            ],
        }

        with patch("requests.get") as mock_get:
            mock_get.return_value.json.return_value = mock_response

            result = self.dataProvider.fetch_data(
                SimulationInput(
                    start_time="2025-02-19T14:12:00+00:00",
                    time_length=86400,
                )
            )

            assert result == [
                {
                    "time": "2025-02-19T14:17:00+00:00",
                    "price": 105000.55,
                },
                {
                    "time": "2025-02-19T14:22:00+00:00",
                    "price": 107995.889,
                },
            ]

    def test_fetch_data_gap_in_the_middle(self):
        # 1739974320 - 2025-02-20T14:12:00+00:00
        # 1739974380 - 2025-02-20T14:13:00+00:00
        # 1739974440 - 2025-02-20T14:14:00+00:00
        # 1739974500 - 2025-02-20T14:15:00+00:00
        # 1739974560 - 2025-02-20T14:16:00+00:00
        # gap        - 2025-02-20T14:17:00+00:00
        # 1739974680 - 2025-02-20T14:18:00+00:00
        # 1739974740 - 2025-02-20T14:19:00+00:00
        # 1739974800 - 2025-02-20T14:20:00+00:00
        # 1739974860 - 2025-02-20T14:21:00+00:00
        # 1739974920 - 2025-02-20T14:22:00+00:00
        # 1739974980 - 2025-02-20T14:23:00+00:00
        mock_response = {
            "t": [
                1739974320,
                1739974380,
                1739974440,
                1739974500,
                1739974560,
                1739974680,
                1739974740,
                1739974800,
                1739974860,
                1739974920,
                1739974980,
            ],
            "c": [
                100000.23,
                101000.55,
                99000.55,
                102000.55,
                103000.55,
                108000.867,
                108000.867,
                99000.23,
                97123.55,
                105123.345,
                107995.889,
            ],
        }

        with patch("requests.get") as mock_get:
            mock_get.return_value.json.return_value = mock_response

            result = self.dataProvider.fetch_data(
                SimulationInput(
                    start_time="2025-02-19T14:12:00+00:00",
                    time_length=86400,
                )
            )

            assert result == [
                {
                    "time": "2025-02-19T14:12:00+00:00",
                    "price": 100000.23,
                },
                {
                    "time": "2025-02-19T14:22:00+00:00",
                    "price": 105123.345,
                },
            ]

    def test_fetch_data_several_values(self):
        # 1739974320 - 2025-02-20T14:12:00+00:00
        # 1739974380 - 2025-02-20T14:13:00+00:00
        # 1739974440 - 2025-02-20T14:14:00+00:00
        # 1739974500 - 2025-02-20T14:15:00+00:00
        # 1739974560 - 2025-02-20T14:16:00+00:00
        # 1739974620 - 2025-02-20T14:17:00+00:00
        # 1739974680 - 2025-02-20T14:18:00+00:00
        # 1739974740 - 2025-02-20T14:19:00+00:00
        # 1739974800 - 2025-02-20T14:20:00+00:00
        # 1739974860 - 2025-02-20T14:21:00+00:00
        # 1739974920 - 2025-02-20T14:22:00+00:00
        # 1739974980 - 2025-02-20T14:23:00+00:00
        mock_response = {
            "t": [
                1739974320,
                1739974380,
                1739974440,
                1739974500,
                1739974560,
                1739974620,
                1739974680,
                1739974740,
                1739974800,
                1739974860,
                1739974920,
                1739974980,
            ],
            "c": [
                100000.23,
                101000.55,
                99000.55,
                102000.55,
                103000.55,
                105000.55,
                108000.867,
                108000.867,
                99000.23,
                97123.55,
                105123.345,
                107995.889,
            ],
        }

        with patch("requests.get") as mock_get:
            mock_get.return_value.json.return_value = mock_response

            result = self.dataProvider.fetch_data(
                SimulationInput(
                    start_time="2025-02-19T14:12:00+00:00",
                    time_length=86400,
                )
            )

            assert result == [
                {
                    "time": "2025-02-19T14:12:00+00:00",
                    "price": 100000.23,
                },
                {
                    "time": "2025-02-19T14:17:00+00:00",
                    "price": 105000.55,
                },
                {
                    "time": "2025-02-19T14:22:00+00:00",
                    "price": 105123.345,
                },
            ]

    def test_fetch_data(self):
        result = self.dataProvider.fetch_data(
            SimulationInput(
                start_time="2025-02-19T14:12:00+00:00",
                time_length=86400,
            )
        )
        print("result", result)
