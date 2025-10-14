import numpy as np
from properscoring import crps_ensemble

# Define scoring intervals in seconds
scoring_intervals = {
    "5min": 300,  # 5 minutes
    "30min": 1800,  # 30 minutes
    "3hour": 10800,  # 3 hours
    "24hour_abs": 86400,  # 24 hours
}


def get_interval_steps(scoring_interval: int, time_increment: int) -> int:
    """
    Calculate the number of steps in the given scoring interval based on the time increment.
    """
    return int(scoring_interval / time_increment)


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

        # Make sure there are no zero prices in the simulation runs because it will cause a division by zero error
        if np.any(simulation_runs == 0):
            return -1.0, [
                {"error": "Zero price encountered in simulation runs"}
            ]

        # Calculate price changes over intervals
        simulated_changes = calculate_price_changes_over_intervals(
            simulation_runs,
            interval_steps,
            absolute_price,
        )
        real_changes = calculate_price_changes_over_intervals(
            real_price_path.reshape(1, -1),
            interval_steps,
            absolute_price,
        )
        data_blocks = label_observed_blocks(real_changes[0])

        # Not enough observed data -> continue
        if len(data_blocks) == 0:
            continue

        # Calculate CRPS over intervals
        total_increment = 0
        crps_values = 0.0
        for block in np.unique(data_blocks):
            # skip missing value blocks
            if block == -1:
                continue

            simulated_changes_block = simulated_changes[
                :, data_blocks == block
            ]
            real_changes_block = real_changes[:, data_blocks == block]
            num_intervals = simulated_changes_block.shape[1]
            crps_values_block = np.zeros(num_intervals)
            for t in range(num_intervals):
                forecasts = simulated_changes_block[:, t]
                observation = real_changes_block[0, t]
                crps_values_block[t] = crps_ensemble(observation, forecasts)
                if absolute_price:
                    crps_values_block[t] = (
                        crps_values_block[t] / real_price_path[-1] * 10_000
                    )
                crps_values += crps_values_block[t]

                # Append detailed data for this increment
                detailed_crps_data.append(
                    {
                        "Interval": interval_name,
                        "Increment": total_increment + 1,
                        "CRPS": crps_values_block[t],
                    }
                )
                total_increment += 1

        # Total CRPS for this interval
        total_crps_interval = crps_values
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


def label_observed_blocks(arr: np.ndarray) -> np.ndarray:
    """
    Groups blocks of consecutive observed data together.
    Example input/output:
    [1.0, 2.0, np.nan, 4.0, np.nan, np.nan, 7.0, 8.0] -> [0, 0, -1, 1, -1, -1, 2, 2]
    """
    not_nan = ~np.isnan(arr)
    block_start = not_nan & np.concatenate(([True], ~not_nan[:-1]))
    group_numbers = np.cumsum(block_start) - 1
    group_labels = np.where(not_nan, group_numbers, -1)
    return group_labels


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
