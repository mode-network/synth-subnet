import unittest
from datetime import datetime

from simulation.utils.helpers import convert_prices_to_time_format, get_intersecting_arrays, round_time_to_minutes


class TestHelpers(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_convert_prices_to_time_format(self):
        prices = [45.67, 56.78, 34.89, 62.15]
        start_time = "2024-11-20T00:00:00"
        time_increment = 300  # 5 minutes in seconds

        formatted_data = convert_prices_to_time_format(prices, start_time, time_increment)

        self.assertEqual(formatted_data, [
            {"time": "2024-11-20T00:00:00", "price": 45.67},
            {"time": "2024-11-20T00:05:00", "price": 56.78},
            {"time": "2024-11-20T00:10:00", "price": 34.89},
            {"time": "2024-11-20T00:15:00", "price": 62.15}
        ])

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

        intersecting_array1, intersecting_array2 = get_intersecting_arrays(array1, array2)

        self.assertEqual(intersecting_array1, [
            {"time": "2024-11-20T00:05:00", "price": 56.78},
            {"time": "2024-11-20T00:10:00", "price": 34.89}
        ])

        self.assertEqual(intersecting_array2, [
            {"time": "2024-11-20T00:05:00", "price": 56.78},
            {"time": "2024-11-20T00:10:00", "price": 62.15}
        ])

    def test_round_time_to_minutes(self):
        time_increment = 300

        dt_str_1 = datetime.fromisoformat("2024-11-25T19:01:59.940515")
        dt_str_2 = datetime.fromisoformat("2024-11-25T19:03:59.940515")

        result_1 = round_time_to_minutes(dt_str_1, time_increment)
        result_2 = round_time_to_minutes(dt_str_2, time_increment)

        self.assertEqual(result_1, "2024-11-25T19:05:00")
        self.assertEqual(result_2, "2024-11-25T19:05:00")
