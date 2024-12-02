from simulation.simulations.price_simulation import simulate_crypto_price_paths, get_asset_price
from simulation.utils.helpers import get_current_time, convert_prices_to_time_format, round_time_to_minutes


def generate_simulations(
        asset='BTC', start_time=None, time_increment=300, time_length=86400, num_simulations=1, sigma=0.01):
    """
    Generate simulated price paths.

    Parameters:
        asset (str): The asset to simulate. Default is 'BTC'.
        start_time (datetime): The start time of the simulation. Defaults to current time.
        time_increment (int): Time increment in seconds.
        time_length (int): Total time length in seconds.
        num_simulations (int): Number of simulation runs.
        sigma (float): Standard deviation of the simulated price path.

    Returns:
        numpy.ndarray: Simulated price paths.
    """
    if start_time is None:
        start_time = get_current_time()

    start_time = round_time_to_minutes(start_time, 60)

    current_price = get_asset_price(asset)
    if current_price is None:
        raise ValueError(f"Failed to fetch current price for asset: {asset}")

    simulations = simulate_crypto_price_paths(
        current_price=current_price,
        time_increment=time_increment,
        time_length=time_length,
        num_simulations=num_simulations,
        sigma=sigma
    )

    predictions = convert_prices_to_time_format(simulations.tolist(), start_time, time_increment)

    return predictions


def generate_fixed_simulation(asset='BTC', start_time=None, time_increment=300, time_length=86400, num_simulations=1, sigma=0.01):
    """
    Generate constant results. Method is used just for test. Don't use in a real simulation.

    Parameters:
        asset (str): The asset to simulate. Default is 'BTC'.
        start_time (datetime): The start time of the simulation. Defaults to current time.
        time_increment (int): Time increment in seconds.
        time_length (int): Total time length in seconds.
        num_simulations (int): Number of simulation runs.
        sigma (float): Standard deviation of the simulated price path.

    Returns:
        numpy.ndarray: Simulated price paths.
    """

    simulations = [[1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000]]

    predictions = convert_prices_to_time_format(simulations, start_time, time_increment)

    return predictions
