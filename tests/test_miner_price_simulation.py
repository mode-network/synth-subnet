import unittest
from unittest.mock import patch, MagicMock

from synth.miner import price_simulation
from synth.miner.price_simulation import (
    LAZER_FEED_ID_MAP,
    get_asset_price,
)


class TestGetAssetPriceHermes(unittest.TestCase):
    def test_hermes_backend_parses_int_mantissa_and_expo(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "parsed": [{"price": {"price": "7930115688547", "expo": "-8"}}]
        }
        with patch.dict("os.environ", {"PYTH_BACKEND": "hermes"}):
            with patch("requests.get", return_value=mock_resp) as mock_get:
                price = get_asset_price("BTC")
                called_url = mock_get.call_args[0][0]

        assert called_url == price_simulation.pyth_base_url
        assert price == 79301.15688547


class TestGetAssetPriceProLazer(unittest.TestCase):
    def test_pro_backend_posts_lazer_with_bearer(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "parsed": {
                "priceFeeds": [
                    {
                        "priceFeedId": LAZER_FEED_ID_MAP["BTC"],
                        "price": "7930115688547",
                        "exponent": -8,
                    }
                ]
            }
        }

        env = {"PYTH_BACKEND": "pro", "PYTH_API_KEY": "test-token"}
        with patch.dict("os.environ", env):
            with patch("requests.post", return_value=mock_resp) as mock_post:
                price = get_asset_price("BTC")

        assert price == 79301.15688547

        called_url = mock_post.call_args[0][0]
        assert called_url == price_simulation.lazer_base_url

        kwargs = mock_post.call_args.kwargs
        assert kwargs["headers"] == {"Authorization": "Bearer test-token"}
        body = kwargs["json"]
        assert body["channel"] == "fixed_rate@200ms"
        assert body["priceFeedIds"] == [LAZER_FEED_ID_MAP["BTC"]]
        assert body["parsed"] is True

    def test_pro_backend_routes_wtioil_to_hyperliquid(self):
        # WTIOIL has no working Lazer feed; in the pro branch the miner
        # mirrors the validator and pulls WTIOIL spot from Hyperliquid
        # (same `coin` code as PriceDataProvider.HYPERLIQUID_SYMBOL_MAP).
        from synth.miner.price_simulation import HYPERLIQUID_ASSET_MAP

        assert "WTIOIL" not in LAZER_FEED_ID_MAP
        assert HYPERLIQUID_ASSET_MAP["WTIOIL"] == "xyz:CL"

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = [
            {"t": 1, "c": "64.10"},
            {"t": 2, "c": "65.00"},
        ]
        env = {"PYTH_BACKEND": "pro", "PYTH_API_KEY": "test-token"}
        with patch.dict("os.environ", env):
            with patch("requests.get") as mock_get:
                with patch(
                    "requests.post", return_value=mock_resp
                ) as mock_post:
                    price = get_asset_price("WTIOIL")

        assert price == 65.0

        # Hyperliquid hit, neither Hermes nor Lazer touched.
        mock_get.assert_not_called()
        called_url = mock_post.call_args[0][0]
        assert called_url == price_simulation.hyperliquid_base_url
        body = mock_post.call_args.kwargs["json"]
        assert body["type"] == "candleSnapshot"
        assert body["req"]["coin"] == "xyz:CL"
        assert body["req"]["interval"] == "1m"

    def test_pro_backend_returns_none_when_api_key_missing(self):
        # Missing key returns None *without* raising — tenacity's @retry only
        # acts on exceptions, so call_count must stay at 0 (we don't want 5
        # useless network attempts once a key is later added).
        env = {"PYTH_BACKEND": "pro"}
        with patch.dict("os.environ", env, clear=False):
            # Make sure PYTH_API_KEY is absent.
            with patch.dict("os.environ", {"PYTH_API_KEY": ""}):
                with patch("requests.post") as mock_post:
                    price = get_asset_price("BTC")
        assert price is None
        mock_post.assert_not_called()


if __name__ == "__main__":
    unittest.main()
