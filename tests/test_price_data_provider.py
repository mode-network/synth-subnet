from datetime import datetime
import unittest
from unittest.mock import patch
import numpy as np


from synth.db.models import ValidatorRequest
from synth.validator.price_data_provider import PriceDataProvider


validator_request = ValidatorRequest(
    asset="BTC",
    start_time=datetime.fromisoformat("2025-02-19T14:12:00+00:00"),
    time_length=360,
    time_increment=120,
)


class TestPriceDataProvider(unittest.TestCase):
    def setUp(self):
        self.dataProvider = PriceDataProvider()

    def test_fetch_data_all_prices(self):
        # 1739974320 - 2025-02-19T14:12:00+00:00
        # 1739974380 - 2025-02-19T14:13:00+00:00
        # 1739974440 - 2025-02-19T14:14:00+00:00
        # 1739974500 - 2025-02-19T14:15:00+00:00
        # 1739974560 - 2025-02-19T14:16:00+00:00
        # 1739974620 - 2025-02-19T14:17:00+00:00
        # 1739974680 - 2025-02-19T14:18:00+00:00
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

            result = self.dataProvider.fetch_data(validator_request)

            assert result == [100000.23, 99000.55, 103000.55, 108000.867]

    def test_fetch_data_gap_1(self):
        # 1739974320 - 2025-02-19T14:12:00+00:00
        # gap        - 2025-02-19T14:13:00+00:00
        # gap        - 2025-02-19T14:14:00+00:00
        # gap        - 2025-02-19T14:15:00+00:00
        # gap        - 2025-02-19T14:16:00+00:00
        # 1739974620 - 2025-02-19T14:17:00+00:00
        # 1739974680 - 2025-02-19T14:18:00+00:00
        mock_response = {
            "t": [1739974320, 1739974620, 1739974680],
            "c": [100000.23, 105000.55, 108000.867],
        }

        with patch("requests.get") as mock_get:
            mock_get.return_value.json.return_value = mock_response

            result = self.dataProvider.fetch_data(validator_request)

            assert result == [100000.23, np.nan, np.nan, 108000.867]

    def test_fetch_data_gap_2(self):
        # 1739974320 - 2025-02-19T14:12:00+00:00
        # gap        - 2025-02-19T14:13:00+00:00
        # gap        - 2025-02-19T14:14:00+00:00
        # gap        - 2025-02-19T14:15:00+00:00
        # gap        - 2025-02-19T14:16:00+00:00
        # gap        - 2025-02-19T14:17:00+00:00
        # 1739974680 - 2025-02-19T14:18:00+00:00
        mock_response = {
            "t": [1739974320, 1739974680],
            "c": [100000.23, 108000.867],
        }

        with patch("requests.get") as mock_get:
            mock_get.return_value.json.return_value = mock_response

            validator_request_eth = ValidatorRequest(
                asset="ETH",
                start_time=datetime.fromisoformat("2025-02-19T14:12:00+00:00"),
                time_length=360,
                time_increment=60,
            )
            result = self.dataProvider.fetch_data(validator_request_eth)

            assert result == [
                100000.23,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                108000.867,
            ]

    def test_fetch_data_gap_3(self):
        # 1739974320 - 2025-02-19T14:12:00+00:00
        # gap        - 2025-02-19T14:13:00+00:00
        # gap        - 2025-02-19T14:14:00+00:00
        # gap        - 2025-02-19T14:15:00+00:00
        # gap        - 2025-02-19T14:16:00+00:00
        # gap        - 2025-02-19T14:17:00+00:00
        # 1739974680 - 2025-02-19T14:18:00+00:00
        # 1739974740 - 2025-02-19T14:19:00+00:00
        # 1739974800 - 2025-02-19T14:20:00+00:00
        # 1739974860 - 2025-02-19T14:21:00+00:00
        # gap        - 2025-02-19T14:22:00+00:00
        mock_response = {
            "t": [
                1739974320,
                1739974680,
                1739974740,
                1739974800,
                1739974860,
            ],
            "c": [
                100000.23,
                108000.867,
                99000.23,
                97123.55,
                105123.345,
            ],
        }

        with patch("requests.get") as mock_get:
            mock_get.return_value.json.return_value = mock_response

            validator_request_eth = ValidatorRequest(
                asset="ETH",
                start_time=datetime.fromisoformat("2025-02-19T14:12:00+00:00"),
                time_length=600,
                time_increment=120,
            )

            result = self.dataProvider.fetch_data(validator_request_eth)

            assert result == [
                100000.23,
                np.nan,
                np.nan,
                108000.867,
                97123.55,
                np.nan,
            ]

    def test_fetch_data_no_prices(self):
        mock_response: dict = {
            "t": [],
            "c": [],
        }

        with patch("requests.get") as mock_get:
            mock_get.return_value.json.return_value = mock_response

            result = self.dataProvider.fetch_data(validator_request)

            assert result == []

    def test_fetch_data_gap_from_start(self):
        # gap        - 2025-02-19T14:12:00+00:00
        # gap        - 2025-02-19T14:13:00+00:00
        # gap        - 2025-02-19T14:14:00+00:00
        # gap        - 2025-02-19T14:15:00+00:00
        # gap        - 2025-02-19T14:16:00+00:00
        # gap        - 2025-02-19T14:17:00+00:00
        # 1739974680 - 2025-02-19T14:18:00+00:00
        # 1739974740 - 2025-02-19T14:19:00+00:00
        # 1739974800 - 2025-02-19T14:20:00+00:00
        # 1739974860 - 2025-02-19T14:21:00+00:00
        # 1739974920 - 2025-02-19T14:22:00+00:00
        mock_response = {
            "t": [1739974680, 1739974740, 1739974800, 1739974860, 1739974920],
            "c": [108000.867, 99000.23, 97123.55, 105123.345, 107995.889],
        }

        with patch("requests.get") as mock_get:
            mock_get.return_value.json.return_value = mock_response

            result = self.dataProvider.fetch_data(validator_request)

            assert result == [np.nan, np.nan, np.nan, 108000.867]

    def test_fetch_data_gap_from_start_2(self):
        # gap        - 2025-02-19T14:12:00+00:00
        # 1739974380 - 2025-02-19T14:13:00+00:00
        # 1739974440 - 2025-02-19T14:14:00+00:00
        # 1739974500 - 2025-02-19T14:15:00+00:00
        # 1739974560 - 2025-02-19T14:16:00+00:00
        # 1739974620 - 2025-02-19T14:17:00+00:00
        # 1739974680 - 2025-02-19T14:18:00+00:00
        # 1739974740 - 2025-02-19T14:19:00+00:00
        # 1739974800 - 2025-02-19T14:20:00+00:00
        # 1739974860 - 2025-02-19T14:21:00+00:00
        # 1739974920 - 2025-02-19T14:22:00+00:00
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

            validator_request = ValidatorRequest(
                asset="BTC",
                start_time=datetime.fromisoformat("2025-02-19T14:12:00+00:00"),
                time_length=600,
                time_increment=300,
            )

            result = self.dataProvider.fetch_data(validator_request)

            assert result == [np.nan, 105000.55, 107995.889]

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

            validator_request = ValidatorRequest(
                asset="BTC",
                start_time=datetime.fromisoformat("2025-02-19T14:12:00+00:00"),
                time_length=600,
                time_increment=300,
            )

            result = self.dataProvider.fetch_data(validator_request)

            assert result == [100000.23, np.nan, 105123.345]

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

            validator_request = ValidatorRequest(
                asset="BTC",
                start_time=datetime.fromisoformat("2025-02-19T14:12:00+00:00"),
                time_length=600,
                time_increment=300,
            )

            result = self.dataProvider.fetch_data(validator_request)

            assert result == [100000.23, 105000.55, 105123.345]

    def test_fetch_data(self):
        result = self.dataProvider.fetch_data(validator_request)
        print("result", result)
