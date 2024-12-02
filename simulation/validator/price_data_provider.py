import requests

from simulation.utils.helpers import from_iso_to_unix_time
from datetime import datetime


class PriceDataProvider:
    BASE_URL = "https://benchmarks.pyth.network/v1/shims/tradingview/history"

    TOKEN_MAP = {
        "BTC": "Crypto.BTC/USD",
        "ETH": "Crypto.ETH/USD"
    }

    one_day_seconds = 24 * 60 * 60

    def __init__(self, token):
        self.token = self._get_token_mapping(token)

    def fetch_data(self, time_point: str):
        """
        Fetch real prices data from an external REST service.
        Returns an array of time points with prices.

        :return: List of dictionaries with 'time' and 'price' keys.
        """

        end_time = from_iso_to_unix_time(time_point)
        start_time = end_time - self.one_day_seconds

        params = {
            "symbol": self.token,
            "resolution": 1,
            "from": start_time,
            "to": end_time
        }

        response = requests.get(self.BASE_URL, params=params)
        response.raise_for_status()

        data = response.json()
        transformed_data = self._transform_data(data)

        return transformed_data

    @staticmethod
    def _transform_data(data):
        if data is None or len(data) == 0:
            return []

        timestamps = data["t"]
        close_prices = data["c"]

        transformed_data = [
            {
                "time": datetime.utcfromtimestamp(timestamps[i]).isoformat(),
                "price": float(close_prices[i])
            }
            for i in range(len(timestamps) - 1, -1, -5)
        ][::-1]

        return transformed_data

    @staticmethod
    def _get_token_mapping(token: str) -> str:
        """
        Retrieve the mapped value for a given token.
        If the token is not in the map, raise an exception or return None.
        """
        if token in PriceDataProvider.TOKEN_MAP:
            return PriceDataProvider.TOKEN_MAP[token]
        else:
            raise ValueError(f"Token '{token}' is not supported.")
