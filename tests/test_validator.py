import unittest
from unittest.mock import patch
from datetime import datetime, timezone


from synth.utils.thread_scheduler import ThreadScheduler
from synth.validator.prompt_config import HIGH_FREQUENCY, LOW_FREQUENCY


class TestValidator(unittest.TestCase):
    def test_select_delay(self):
        cycle_start_time = datetime(
            2025, 12, 3, 12, 34, 56, 998, tzinfo=timezone.utc
        )

        # Test high frequency
        with patch(
            "synth.utils.thread_scheduler.get_current_time"
        ) as mock_get_current_time:
            mock_get_current_time.return_value = datetime(
                2025, 12, 3, 12, 36, 30, tzinfo=timezone.utc
            )
            delay = ThreadScheduler.select_delay(
                HIGH_FREQUENCY.asset_list,
                cycle_start_time,
                HIGH_FREQUENCY,
            )
            self.assertEqual(delay, 30)

            delay = ThreadScheduler.select_delay(
                LOW_FREQUENCY.asset_list,
                cycle_start_time,
                LOW_FREQUENCY,
            )
            self.assertEqual(delay, 270)

    def test_select_asset(self):
        latest_asset = "ETH"
        asset_list = ["BTC", "ETH", "LTC"]

        selected_asset = ThreadScheduler.select_asset(latest_asset, asset_list)
        self.assertEqual(selected_asset, "LTC")
