import datetime


import numpy as np


from synth.validator.price_data_provider import PriceDataProvider


def get_asset_price(asset="BTC"):
    """
    Retrieves the current price of the specified asset.
    Currently, supports BTC via Pyth Network.

    Returns:
        float: Current asset price.
    """
    price_data_provider = PriceDataProvider()
    prices = price_data_provider.fetch_data(
        asset,
        (
            datetime.datetime.now(datetime.timezone.utc)
            - datetime.timedelta(minutes=2)
        ).isoformat(),
        60 * 2,
        False,
    )

    return prices["c"][-1]


def simulate_single_price_path(
    current_price, time_increment, time_length, sigma
):
    """
    Simulate a single crypto asset price path.
    """
    one_hour = 3600
    dt = time_increment / one_hour
    num_steps = int(time_length / time_increment)
    std_dev = sigma * np.sqrt(dt)
    price_change_pcts = np.random.normal(0, std_dev, size=num_steps)
    cumulative_returns = np.cumprod(1 + price_change_pcts)
    cumulative_returns = np.insert(cumulative_returns, 0, 1.0)
    price_path = current_price * cumulative_returns
    return price_path


def simulate_crypto_price_paths(
    current_price, time_increment, time_length, num_simulations, sigma
):
    """
    Simulate multiple crypto asset price paths.
    """

    price_paths = []
    for _ in range(num_simulations):
        price_path = simulate_single_price_path(
            current_price, time_increment, time_length, sigma
        )
        price_paths.append(price_path)

    return np.array(price_paths)
