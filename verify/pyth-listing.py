import requests

if __name__ == "__main__":
    urls = [
        "https://benchmarks.pyth.network/v1/shims/tradingview/symbol_info?group=pyth_stock",
        "https://benchmarks.pyth.network/v1/shims/tradingview/symbol_info?group=pyth_crypto",
    ]
    assets = []
    for url in urls:
        response = requests.get(url)
        data = response.json()

        for item in zip(
            data["symbol"],
            data["description"],
            data["session-regular"],
            data["base-currency"],
            data["ticker"],
            data["supported-resolutions"],
        ):
            if item[0] in [
                "SOLUSD",
                "GLD",
                "SIVR",
                "AAPL",
                "IVV",
                "TSLA",
            ] or item[3] in ["BTC", "ETH", "TAO", "MODE"]:
                assets.append(
                    {
                        "symbol": item[0],
                        "description": item[1],
                        "session-regular": item[2],
                        "base-currency": item[3],
                        "ticker": item[4],
                        "supported-resolutions": item[5],
                    }
                )

    print(assets)
