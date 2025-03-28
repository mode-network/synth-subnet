import logging
import requests
from datetime import datetime, timezone


from tenacity import (
    before_log,
    retry,
    stop_after_attempt,
    wait_random_exponential,
)
import bittensor as bt


from synth.utils.helpers import from_iso_to_unix_time


class PriceDataProvider:
    BASE_URL = "https://benchmarks.pyth.network/v1/shims/tradingview/history"

    TOKEN_MAP = {"BTC": "Crypto.BTC/USD", "ETH": "Crypto.ETH/USD"}

    def __init__(self, token):
        self.token = self._get_token_mapping(token)

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_random_exponential(multiplier=5),
        reraise=True,
        before=before_log(bt.logging._logger, logging.DEBUG),
    )
    def fetch_data(self, start_time: str, time_length: int):
        """
        Fetch real prices data from an external REST service.
        Returns an array of time points with prices.

        :return: List of dictionaries with 'time' and 'price' keys.
        """

        start_time = from_iso_to_unix_time(start_time)
        end_time = start_time + time_length

        params = {
            "symbol": self.token,
            "resolution": 1,
            "from": start_time,
            "to": end_time,
        }

        response = requests.get(self.BASE_URL, params=params)
        response.raise_for_status()

        data = response.json()
        transformed_data = self._transform_data(data, start_time)

        return transformed_data

    @staticmethod
    def _transform_data(data, start_time):
        if data is None or len(data) == 0:
            return []

        timestamps = data["t"]
        close_prices = data["c"]

        transformed_data = []

        for t, c in zip(timestamps, close_prices):
            if (
                t >= start_time and (t - start_time) % 300 == 0
            ):  # 300s = 5 minutes
                transformed_data.append(
                    {
                        "time": datetime.fromtimestamp(
                            t, timezone.utc
                        ).isoformat(),
                        "price": float(c),
                    }
                )

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
