import unittest
from unittest.mock import patch
from datetime import datetime, timezone


from neurons.validator import Validator
from synth.validator.prompt_config import HIGH_FREQUENCY, LOW_FREQUENCY


class TestValidator(unittest.TestCase):
    def test_select_delay_immediate(self):
        cycle_start_time = datetime(2025, 12, 3, 12, 0, 0)

        # Test high frequency
        delay = Validator.select_delay(
            Validator.asset_list, cycle_start_time, HIGH_FREQUENCY, True
        )
        self.assertEqual(delay, 0)

        # Test low frequency
        delay = Validator.select_delay(
            Validator.asset_list, cycle_start_time, LOW_FREQUENCY, True
        )
        self.assertEqual(delay, 60)

    def test_select_delay(self):
        cycle_start_time = datetime(
            2025, 12, 3, 12, 34, 56, 998, tzinfo=timezone.utc
        )

        # Test high frequency
        with patch(
            "neurons.validator.get_current_time"
        ) as mock_get_current_time:
            mock_get_current_time.return_value = datetime(
                2025, 12, 3, 12, 36, 30, tzinfo=timezone.utc
            )
            delay = Validator.select_delay(
                Validator.asset_list, cycle_start_time, HIGH_FREQUENCY, False
            )
            self.assertEqual(delay, 90)

    def test_select_asset(self):
        latest_asset = "ETH"
        asset_list = ["BTC", "ETH", "LTC"]

        with patch(
            "neurons.validator.get_current_time"
        ) as mock_get_current_time:
            mock_get_current_time.return_value = datetime(
                2025, 12, 3, 12, 36, 30, tzinfo=timezone.utc
            )
            selected_asset = Validator.select_asset(
                latest_asset, asset_list, 0
            )
            self.assertEqual(selected_asset, "LTC")

    def test_select_asset_gold(self):
        latest_asset = "ETH"
        asset_list = ["BTC", "ETH", "XAU", "LTC"]

        with patch(
            "neurons.validator.get_current_time"
        ) as mock_get_current_time:
            mock_get_current_time.return_value = datetime(
                2025, 12, 3, 12, 36, 30, tzinfo=timezone.utc
            )
            with patch(
                "neurons.validator.should_skip_xau"
            ) as mock_should_skip_xau:
                mock_should_skip_xau.return_value = True
                selected_asset = Validator.select_asset(
                    latest_asset, asset_list, 0
                )
                self.assertEqual(selected_asset, "LTC")
