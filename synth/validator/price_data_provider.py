import logging
import time
import requests


from tenacity import (
    before_log,
    retry,
    stop_after_attempt,
    wait_random_exponential,
)
import numpy as np
import bittensor as bt

from synth.db.models import ValidatorRequest
from synth.utils.helpers import from_iso_to_unix_time
from synth.utils.logging import print_execution_time

# Pyth API benchmarks doc: https://benchmarks.pyth.network/docs
# get the list of stocks supported by pyth: https://benchmarks.pyth.network/v1/shims/tradingview/symbol_info?group=pyth_stock
# get the list of crypto supported by pyth: https://benchmarks.pyth.network/v1/shims/tradingview/symbol_info?group=pyth_crypto
# get the ticker: https://benchmarks.pyth.network/v1/shims/tradingview/symbols?symbol=Crypto.XAUT/USD


class PriceDataProvider:
    PYTH_BASE_URL = (
        "https://benchmarks.pyth.network/v1/shims/tradingview/history"
    )
    HYPERLIQUID_BASE_URL = "https://api.hyperliquid.xyz/info"

    # Assets fetched from Pyth
    PYTH_SYMBOL_MAP = {
        "BTC": "Crypto.BTC/USD",
        "ETH": "Crypto.ETH/USD",
        "XAU": "Crypto.XAUT/USD",
        "SOL": "Crypto.SOL/USD",
        "SPYX": "Crypto.SPYX/USD",
        "NVDAX": "Crypto.NVDAX/USD",
        "TSLAX": "Crypto.TSLAX/USD",
        "AAPLX": "Crypto.AAPLX/USD",
        "GOOGLX": "Crypto.GOOGLX/USD",
        "XRP": "Crypto.XRP/USD",
        "HYPE": "Crypto.HYPE/USD",
    }

    # Assets fetched from Hyperliquid (overrides Pyth for these assets)
    HYPERLIQUID_SYMBOL_MAP = {
        "WTIOIL": "xyz:CL",
    }

    @staticmethod
    def assert_assets_supported(asset_list: list[str]):
        supported = (
            PriceDataProvider.PYTH_SYMBOL_MAP.keys()
            | PriceDataProvider.HYPERLIQUID_SYMBOL_MAP.keys()
        )
        for asset in asset_list:
            assert asset in supported

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_random_exponential(multiplier=2),
        reraise=True,
        before=before_log(bt.logging._logger, logging.DEBUG),
    )
    @print_execution_time
    def fetch_data(self, validator_request: ValidatorRequest) -> list:
        """
        Fetch price data for the given request.
        Returns a list of close prices (float or NaN) aligned to the
        timestamp grid defined by start_time, time_length, and time_increment.
        """
        asset = str(validator_request.asset)

        if asset in self.HYPERLIQUID_SYMBOL_MAP:
            return self.fetch_data_hyperliquid(validator_request)

        start_time_int = from_iso_to_unix_time(
            validator_request.start_time.isoformat()
        )
        params = {
            "symbol": self.PYTH_SYMBOL_MAP[asset],
            "resolution": 1,
            "from": start_time_int,
            "to": start_time_int + validator_request.time_length,
        }

        response = requests.get(self.PYTH_BASE_URL, params=params)
        response.raise_for_status()
        data = response.json()

        return self._transform_data(
            data,
            start_time_int,
            int(validator_request.time_increment),
            int(validator_request.time_length),
        )

    def fetch_data_hyperliquid(
        self, validator_request: ValidatorRequest
    ) -> list:
        start_time_int = from_iso_to_unix_time(
            validator_request.start_time.isoformat()
        )
        return self.download_hyperliquid_price_data(
            beginning=start_time_int,
            end=start_time_int + int(validator_request.time_length),
            symbol=str(validator_request.asset),
            time_increment=int(validator_request.time_increment),
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_random_exponential(multiplier=2),
        reraise=True,
        before=before_log(bt.logging._logger, logging.DEBUG),
    )
    def download_hyperliquid_price_data(
        self,
        beginning: int,  # Unix timestamp in seconds
        end: int,  # Unix timestamp in seconds
        symbol: str = "WTIOIL",
        time_increment: int = 60,
        loop_wait_time_seconds: float = 0.1,
    ) -> list:
        MAX_CANDLES = 5000
        INTERVAL_MS = 60 * 1000  # 1 minute in ms
        chunk_ms = MAX_CANDLES * INTERVAL_MS

        beginning_ms = beginning * 1000
        end_ms = end * 1000
        candles = []

        with requests.Session() as session:
            current_start = beginning_ms
            while current_start < end_ms:
                current_end = min(current_start + chunk_ms, end_ms)

                payload = {
                    "type": "candleSnapshot",
                    "req": {
                        "coin": self.HYPERLIQUID_SYMBOL_MAP[symbol],
                        "interval": "1m",
                        "startTime": current_start,
                        "endTime": current_end,
                    },
                }

                response = session.post(
                    self.HYPERLIQUID_BASE_URL, json=payload, timeout=100
                )
                response.raise_for_status()
                data = response.json()

                bt.logging.debug(
                    f"Fetched {len(data)} candles for {symbol} [{current_start}, {current_end}]"
                )

                for candle in data:
                    if beginning_ms <= int(candle["t"]) <= end_ms:
                        candles.append(candle)

                current_start += chunk_ms
                time.sleep(loop_wait_time_seconds)

        if not candles:
            bt.logging.warning(
                f"No data returned from Hyperliquid for {symbol}"
            )
            return []

        normalized = {
            "t": [candle["t"] // 1000 for candle in candles],
            "c": [float(candle["c"]) for candle in candles],
        }
        return self._transform_data(
            normalized, beginning, time_increment, end - beginning
        )

    @staticmethod
    def _transform_data(
        data, start_time_int: int, time_increment: int, time_length: int
    ) -> list:
        if data is None or len(data) == 0 or len(data["t"]) == 0:
            return []

        time_end_int = start_time_int + time_length
        timestamps = list(
            range(
                start_time_int, time_end_int + time_increment, time_increment
            )
        )

        if len(timestamps) != int(time_length / time_increment) + 1:
            # Note: this part of code should never be activated; just included for precaution
            if len(timestamps) == int(time_length / time_increment) + 2:
                if data["t"][-1] < timestamps[1]:
                    timestamps = timestamps[:-1]
                elif data["t"][0] > timestamps[0]:
                    timestamps = timestamps[1:]
            else:
                return []

        close_prices_dict = {t: c for t, c in zip(data["t"], data["c"])}
        result = [np.nan] * len(timestamps)
        for idx, t in enumerate(timestamps):
            if t in close_prices_dict:
                result[idx] = close_prices_dict[t]

        return result
