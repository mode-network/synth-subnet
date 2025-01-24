from synth.miner.price_simulation import (
    simulate_crypto_price_paths,
    get_asset_price,
)
from synth.utils.helpers import (
    convert_prices_to_time_format,
)


def generate_simulations(
    asset="BTC",
    start_time=None,
    time_increment=300,
    time_length=86400,
    num_simulations=1,
):
    """
    Generate simulated price paths.

    Parameters:
        asset (str): The asset to simulate. Default is 'BTC'.
        start_time (str): The start time of the simulation. Defaults to current time.
        time_increment (int): Time increment in seconds.
        time_length (int): Total time length in seconds.
        num_simulations (int): Number of simulation runs.

    Returns:
        numpy.ndarray: Simulated price paths.
    """
    if start_time is None:
        raise ValueError("Start time must be provided.")

    current_price = get_asset_price(asset)
    if current_price is None:
        raise ValueError(f"Failed to fetch current price for asset: {asset}")

    # Standard deviation of the simulated price path
    sigma = 0.01

    simulations = simulate_crypto_price_paths(
        current_price=current_price,
        time_increment=time_increment,
        time_length=time_length,
        num_simulations=num_simulations,
        sigma=sigma,
    )

    predictions = convert_prices_to_time_format(
        simulations.tolist(), start_time, time_increment
    )

    return predictions
