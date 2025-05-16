import numpy as np
from properscoring import crps_ensemble


def calculate_crps_for_miner(
    simulation_runs: np.ndarray,
    real_price_path: np.ndarray,
    time_increment: int,
) -> tuple[float, list[dict]]:
    """
    Calculate the total CRPS score for a miner's simulations over specified intervals,
    Return the sum of the scores.

    Parameters:
        simulation_runs (numpy.ndarray): Simulated price paths.
        real_price_path (numpy.ndarray): The real price path.
        time_increment (int): Time increment in seconds.

    Returns:
        float: Sum of total CRPS scores over the intervals.
    """
    # Define scoring intervals in seconds
    scoring_intervals = {
        "5min": 300,  # 5 minutes
        "30min": 1800,  # 30 minutes
        "3hour": 10800,  # 3 hours
        "24hour_abs": 86400,  # 24 hours
    }

    # Function to calculate interval steps
    def get_interval_steps(scoring_interval: int, time_increment: int) -> int:
        return int(scoring_interval / time_increment)

    # Initialize lists to store detailed CRPS data
    detailed_crps_data: list[dict] = []

    # Sum of all scores
    sum_all_scores = 0.0

    for interval_name, interval_seconds in scoring_intervals.items():
        interval_steps = get_interval_steps(interval_seconds, time_increment)
        absolute_price = interval_name.endswith("_abs")

        # If we are considering absolute prices, adjust the interval steps for potential gaps:
        # if only the initial price is present, then decrease the interval step
        if absolute_price:
            while (
                real_price_path[::interval_steps].shape[0] == 1
                and interval_steps > 1
            ):
                interval_steps -= 1

        # Calculate price changes over intervals
        simulated_changes = calculate_price_changes_over_intervals(
            simulation_runs,
            interval_steps,
            absolute_price=absolute_price,
        )
        real_changes = calculate_price_changes_over_intervals(
            real_price_path.reshape(1, -1),
            interval_steps,
            absolute_price=absolute_price,
        )

        # Calculate CRPS over intervals
        num_intervals = simulated_changes.shape[1]
        crps_values = np.zeros(num_intervals)
        for t in range(num_intervals):
            forecasts = simulated_changes[:, t]
            observation = real_changes[0, t]
            crps_values[t] = crps_ensemble(observation, forecasts)
            if absolute_price:
                crps_values[t] = crps_values[t] / real_price_path[-1] * 10_000

            # Append detailed data for this increment
            detailed_crps_data.append(
                {
                    "Interval": interval_name,
                    "Increment": t + 1,
                    "CRPS": crps_values[t],
                }
            )

        # Total CRPS for this interval
        total_crps_interval = np.sum(crps_values)
        sum_all_scores += float(total_crps_interval)

        # Append total CRPS for this interval to detailed data
        detailed_crps_data.append(
            {
                "Interval": interval_name,
                "Increment": "Total",
                "CRPS": total_crps_interval,
            }
        )

    # Append overall total CRPS to detailed data
    detailed_crps_data.append(
        {"Interval": "Overall", "Increment": "Total", "CRPS": sum_all_scores}
    )

    # Return the sum of all scores
    return sum_all_scores, detailed_crps_data


def calculate_price_changes_over_intervals(
    price_paths: np.ndarray, interval_steps: int, absolute_price=False
) -> np.ndarray:
    """
    Calculate price changes over specified intervals.

    Parameters:
        price_paths (numpy.ndarray): Array of simulated price paths.
        interval_steps (int): Number of steps that make up the interval.
        absolute_price (bool): If True, absolute price values (rather than price changes) are returned.

    Returns:
        numpy.ndarray: Array of price changes over intervals.
    """
    # Get the prices at the interval points
    interval_prices = price_paths[:, ::interval_steps]
    # Calculate price changes over intervals
    if absolute_price:
        return interval_prices[:, 1:]

    return (
        np.diff(interval_prices, axis=1) / interval_prices[:, :-1]
    ) * 10_000
