import logging
import requests


from tenacity import (
    before_log,
    retry,
    stop_after_attempt,
    wait_random_exponential,
)
import bittensor as bt


from synth.db.models import ValidatorRequest
from synth.utils.helpers import from_iso_to_unix_time

# Pyth API benchmarks doc: https://benchmarks.pyth.network/docs
# get the list of stocks supported by pyth: https://benchmarks.pyth.network/v1/shims/tradingview/symbol_info?group=pyth_stock
# get the list of crypto supported by pyth: https://benchmarks.pyth.network/v1/shims/tradingview/symbol_info?group=pyth_crypto
# get the ticket: https://benchmarks.pyth.network/v1/shims/tradingview/symbols?symbol=Metal.XAU/USD


class PriceDataProvider:
    BASE_URL = "https://benchmarks.pyth.network/v1/shims/tradingview/history"

    TOKEN_MAP = {
        "BTC": "Crypto.BTC/USD",
        "ETH": "Crypto.ETH/USD",
        "XAU": "Metal.XAU/USD",
        "SOL": "Crypto.SOL/USD",
    }

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_random_exponential(multiplier=7),
        reraise=True,
        before=before_log(bt.logging._logger, logging.DEBUG),
    )
    def fetch_data(self, validator_request: ValidatorRequest) -> list[dict]:
        """
        Fetch real prices data from an external REST service.
        Returns an array of time points with prices.

        :return: List of dictionaries with 'time' and 'price' keys.
        """

        start_time_int = from_iso_to_unix_time(
            validator_request.start_time.isoformat()
        )
        end_time_int = start_time_int + validator_request.time_length

        params = {
            "symbol": self._get_token_mapping(validator_request.asset),
            "resolution": 1,
            "from": start_time_int,
            "to": end_time_int,
        }

        response = requests.get(self.BASE_URL, params=params)
        response.raise_for_status()

        data = response.json()

        transformed_data = self._transform_data(
            data, start_time_int, validator_request.time_increment
        )

        return transformed_data

    @staticmethod
    def _transform_data(
        data, start_time_int: int, time_increment: int
    ) -> list:
        if data is None or len(data) == 0:
            return []

        timestamps = data["t"]
        close_prices = data["c"]

        transformed_data = []

        if len(timestamps) == 0:
            return []

        for t, c in zip(timestamps, close_prices):
            if (
                t >= start_time_int
                and (t - start_time_int) % time_increment == 0
            ):
                transformed_data.append(float(c))

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
