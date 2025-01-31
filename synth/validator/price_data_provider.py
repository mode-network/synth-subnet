import requests

from synth.utils.helpers import from_iso_to_unix_time
from datetime import datetime, timezone


class PriceDataProvider:
    BASE_URL = "https://benchmarks.pyth.network/v1/shims/tradingview/history"
    DEFAULT_TIME_INTERVAL = 5  # in seconds
    DEFAULT_TIME_LENGTH = 86400  # 24 hours in seconds
    TOKEN_MAP = {"BTC": "Crypto.BTC/USD", "ETH": "Crypto.ETH/USD"}

    def __init__(self, token, time_length=None, time_interval=None):
        """
        Initializes the price data provider.

        :param token: The symbol of the token to use. E.g. `BTC`.
        :param time_length: A time length period, in seconds, of the price data to fetch. Defaults to 24 hours (in seconds).
        :param time_interval: A time interval, in seconds, between each price point. Defaults to 5 seconds.
        """
        self.time_length = (
            time_length
            if time_length and time_length > 0
            else PriceDataProvider.DEFAULT_TIME_LENGTH
        )
        self.time_interval = (
            time_interval
            if time_interval and time_interval > 0
            else PriceDataProvider.DEFAULT_TIME_INTERVAL
        )
        self.token = self._get_token_mapping(token)

    def fetch_data(self, iso_start_time: str):
        """
        Fetch real prices data from an external REST service.
        Returns an array of time points with prices.

        :param iso_start_time: The time, in ISO 8601 format, to start fetch the data from.
        :return: List of dictionaries with 'time' and 'price' keys.
        """

        end_time = from_iso_to_unix_time(iso_start_time)
        start_time = end_time - self.time_length

        params = {
            "symbol": self.token,
            "resolution": 1,
            "from": start_time,
            "to": end_time,
        }

        response = requests.get(self.BASE_URL, params=params)
        response.raise_for_status()

        data = response.json()
        transformed_data = self._transform_data(data, self.time_interval)

        return transformed_data

    @staticmethod
    def _transform_data(data, time_interval=None):
        if data is None or len(data) == 0:
            return []

        timestamps = data["t"]
        close_prices = data["c"]

        transformed_data = [
            {
                "time": datetime.fromtimestamp(
                    timestamps[i], timezone.utc
                ).isoformat(),
                "price": float(close_prices[i]),
            }
            for i in range(
                len(timestamps) - 1,
                -1,
                -(
                    time_interval
                    if time_interval and time_interval > 0
                    else PriceDataProvider.DEFAULT_TIME_INTERVAL
                ),
            )
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
