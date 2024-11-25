import random
from datetime import datetime, timedelta
import bittensor as bt

import requests

from simulation.utils.helpers import from_iso_to_unix_time


class PriceDataProvider:
    BASE_URL = "https://ref.mode.network/tokens/historical/rates"

    TOKEN_MAP = {
        "BTC": "pyth-btc",
        "ETH": "pyth-eth"
    }

    def __init__(self, token, validation_time_str):
        token = self._get_token_mapping(token)
        validation_time = from_iso_to_unix_time(validation_time_str)
        self.params = {"token": token, "time": validation_time}

    def fetch_data(self):
        """
        Fetch real prices data from an external REST service.
        Returns an array of time points with prices.

        :return: List of dictionaries with 'time' and 'price' keys.
        """

        bt.logging.info(f"Fetching data from {self.BASE_URL} with params {self.params}")

        response = requests.get(self.BASE_URL, params=self.params)
        response.raise_for_status()

        data = response.json()
        transformed_data = self._transform_data(data)

        return transformed_data

    @staticmethod
    def _transform_data(data):
        transformed_data = []

        for entry in data:
            # Parse the updatedAt field and round it to the nearest minute
            updated_at = datetime.strptime(entry["updatedAt"], "%Y-%m-%dT%H:%M:%S.%fZ")
            rounded_time = updated_at.replace(second=0, microsecond=0)

            # Convert usdRate to a float
            price = float(entry["usdRate"])

            # Add the transformed entry to the result list
            transformed_data.append({
                "time": rounded_time.isoformat(),
                "price": price
            })

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
