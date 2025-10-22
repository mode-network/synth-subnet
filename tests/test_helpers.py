import unittest
from datetime import datetime, timedelta, timezone

from synth.utils.helpers import (
    convert_prices_to_time_format,
    get_intersecting_arrays,
    round_time_to_minutes,
    from_iso_to_unix_time,
    get_current_time,
    round_to_8_significant_digits,
    timeout_until,
)


class TestHelpers(unittest.TestCase):
    def test_get_current_time(self):
        current_time = get_current_time()
        print("current_time", current_time)

    def test_round_to_8_significant_digits(self):
        assert (
            round_to_8_significant_digits(123456.789) == 123456.79
        )  # Rounds to 8 significant digits
        assert (
            round_to_8_significant_digits(123) == 123
        )  # Stays the same, already 3 significant digits
        assert round_to_8_significant_digits(0.000123456789) == 0.00012345679
        assert round_to_8_significant_digits(0.0) == 0.0

    def test_convert_prices_to_time_format(self):
        prices = [[45.67, 56.78, 34.89, 62.15]]
        start_time = "2024-11-19T23:00:00"
        time_increment = 300  # 5 minutes in seconds

        formatted_data = convert_prices_to_time_format(
            prices, start_time, time_increment
        )

        self.assertEqual(
            formatted_data,
            (1732057200, 300, [45.67, 56.78, 34.89, 62.15]),
        )

    def test_get_intersecting_arrays(self):
        array1 = [
            {"time": "2024-11-20T00:00:00", "price": 45.67},
            {"time": "2024-11-20T00:05:00", "price": 56.78},
            {"time": "2024-11-20T00:10:00", "price": 34.89},
        ]

        array2 = [
            {"time": "2024-11-20T00:05:00", "price": 56.78},
            {"time": "2024-11-20T00:10:00", "price": 62.15},
            {"time": "2024-11-20T00:15:00", "price": 75.20},
        ]

        intersecting_array1, intersecting_array2 = get_intersecting_arrays(
            array1, array2
        )

        self.assertEqual(
            intersecting_array1,
            [
                {"time": "2024-11-20T00:05:00", "price": 56.78},
                {"time": "2024-11-20T00:10:00", "price": 34.89},
            ],
        )

        self.assertEqual(
            intersecting_array2,
            [
                {"time": "2024-11-20T00:05:00", "price": 56.78},
                {"time": "2024-11-20T00:10:00", "price": 62.15},
            ],
        )

    def test_round_time_to_minutes(self):
        time_increment = 60

        self.assertEqual(
            round_time_to_minutes(
                datetime.fromisoformat("2024-11-25T19:01:59.940515"),
                time_increment,
            ).isoformat(),
            "2024-11-25T19:02:00",
        )
        self.assertEqual(
            round_time_to_minutes(
                datetime.fromisoformat("2024-11-25T19:03:01.940515"),
                time_increment,
            ).isoformat(),
            "2024-11-25T19:04:00",
        )

    def test_round_time_to_five_minutes(self):
        time_increment = 300

        dt_str_1 = "2024-11-25T19:01:59.940515"
        dt_str_2 = "2024-11-25T19:03:59.940515"

        result_1 = round_time_to_minutes(
            datetime.fromisoformat(dt_str_1), time_increment
        )
        result_2 = round_time_to_minutes(
            datetime.fromisoformat(dt_str_2), time_increment
        )

        self.assertEqual(result_1.isoformat(), "2024-11-25T19:05:00")
        self.assertEqual(result_2.isoformat(), "2024-11-25T19:05:00")

    def test_round_time_to_minutes_plus_two_extra(self):
        dt_str_1 = "2024-11-25T19:01:59.940515"
        result_1 = round_time_to_minutes(
            datetime.fromisoformat(dt_str_1), 60, 120
        )
        self.assertEqual(result_1.isoformat(), "2024-11-25T19:04:00")

        dt_str_2 = "2024-11-25T19:03:09.659353"
        result_2 = round_time_to_minutes(
            datetime.fromisoformat(dt_str_2), 60, 120
        )
        self.assertEqual(result_2.isoformat(), "2024-11-25T19:06:00")

    def test_round_time_to_two_minutes(self):
        time_increment = 120
        extra_seconds = 60

        dt_str_1 = "2024-11-25T19:01:59.940515"
        dt_str_2 = "2024-11-25T19:03:59.940515"

        result_1 = round_time_to_minutes(
            datetime.fromisoformat(dt_str_1), time_increment, extra_seconds
        )
        result_2 = round_time_to_minutes(
            datetime.fromisoformat(dt_str_2), time_increment, extra_seconds
        )

        self.assertEqual(result_1.isoformat(), "2024-11-25T19:03:00")
        self.assertEqual(result_2.isoformat(), "2024-11-25T19:05:00")

    def test_from_iso_to_unix_time(self):
        iso_time = "2024-11-25T16:20:00"

        unix_time = from_iso_to_unix_time(iso_time)

        self.assertEqual(unix_time, 1732551600)

        self.assertEqual(
            from_iso_to_unix_time("2025-08-05T14:56:00+00:00"), 1754405760
        )

    def test_timeout_until(self):
        # Arrange: Set a future time 10 seconds from now
        future_time = datetime.now(timezone.utc) + timedelta(seconds=10)

        # Act: Call the timeout_until function
        timeout = timeout_until(future_time)

        # Assert: The timeout should be approximately 10 seconds
        assert (
            9 <= timeout <= 10
        ), f"Expected timeout to be around 10 seconds, got {timeout}"

    def test_timeout_until_past_time(self):
        # Arrange: Set a past time 10 seconds ago
        past_time = datetime.now(timezone.utc) - timedelta(seconds=10)

        # Act: Call the timeout_until function
        timeout = timeout_until(past_time)

        # Assert: The timeout should be negative
        assert timeout == 0, f"Expected timeout to be 0, got {timeout}"
