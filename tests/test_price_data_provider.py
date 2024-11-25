import unittest

from simulation.validator.price_data_provider import PriceDataProvider


class TestPriceDataProvider(unittest.TestCase):
    def setUp(self):
        self.dataProvider = PriceDataProvider("BTC", "2024-11-25T12:29:48")

    def tearDown(self):
        pass

    def test_fetch_data(self):
        result = self.dataProvider.fetch_data()
        print(result)
