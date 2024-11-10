import numpy as np
import requests
from properscoring import crps_ensemble


def get_asset_price(asset='BTC'):
    """
    Retrieves the current price of the specified asset.
    Currently, supports BTC via Pyth Network.

    Returns:
        float: Current asset price.
    """
    if asset == 'BTC':
        btc_price_id = "btc-price-id"
        endpoint = f"https://hermes.pyth.network/api/latest_price_feeds?ids[]={btc_price_id}"
        try:
            response = requests.get(endpoint)
            response.raise_for_status()
            data = response.json()
            if not data or len(data) == 0:
                raise ValueError("No price data received")
            price_feed = data[0]
            price = float(price_feed['price']['price']) / (10**8)
            return price
        except Exception as e:
            print(f"Error fetching {asset} price: {str(e)}")
            return None
    else:
        # For other assets, implement accordingly
        print(f"Asset '{asset}' not supported.")
        return None


def simulate_single_price_path(current_price, time_increment, time_length, sigma):
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


def generate_real_price_path(current_price, time_increment, time_length, sigma):
    """
    Generate a 'real' price path.
    """
    # No random seed set to ensure independent random numbers
    real_price_path = simulate_single_price_path(current_price, time_increment, time_length, sigma)
    return real_price_path


def simulate_crypto_price_paths(current_price, time_increment, time_length, num_simulations, sigma):
    """
    Simulate multiple crypto asset price paths.
    """

    price_paths = []
    for _ in range(num_simulations):
        price_path = simulate_single_price_path(current_price, time_increment, time_length, sigma)
        price_paths.append(price_path)

    return np.array(price_paths)


def calculate_price_changes(price_paths):
    """
    Calculate the incremental price changes between consecutive time increments.
    """
    return np.diff(price_paths, axis=1)


def calculate_crps_over_time(simulated_values, real_values):
    """
    Calculate the CRPS over time.
    """
    num_time_steps = simulated_values.shape[1]
    crps_values = np.zeros(num_time_steps)
    for t in range(num_time_steps):
        forecasts = simulated_values[:, t]
        observation = real_values[t]
        crps_values[t] = crps_ensemble(observation, forecasts)
    return crps_values


def calculate_cumulative_price_changes(price_paths):
    """
    Calculate the cumulative price changes from the start time to each time increment.
    """
    initial_prices = price_paths[:, [0]]  # Shape: (num_paths, 1)
    cumulative_changes = price_paths - initial_prices  # Broadcasting subtraction
    return cumulative_changes
