import unittest

from simulation.validator.price_data_provider import PriceDataProvider


class TestPriceDataProvider(unittest.TestCase):
    def setUp(self):
        self.dataProvider = PriceDataProvider("BTC", 1732379388)

    def tearDown(self):
        pass

    def test_fetch_data(self):
        result = self.dataProvider.fetch_data()
        print(result)
