from typing import Any, Optional
from datetime import datetime, timedelta, timezone
import numpy as np


def get_current_time() -> datetime:
    # Get current date and time
    return datetime.now(timezone.utc).replace(microsecond=0)


def convert_prices_to_time_format(prices, start_time, time_increment):
    """
    Convert an array of float numbers (prices) into an array of dictionaries with 'time' and 'price'.

    :param prices: List of float numbers representing prices.
    :param start_time: ISO 8601 string representing the start time.
    :param time_increment: Time increment in seconds between consecutive prices.
    :return: List of dictionaries with 'time' and 'price' keys.
    """
    start_time = datetime.fromisoformat(
        start_time
    )  # Convert start_time to a datetime object
    result = []

    for price_item in prices:
        single_prediction = []
        for i, price in enumerate(price_item):
            time_point = start_time + timedelta(seconds=i * time_increment)
            single_prediction.append(
                {"time": time_point.isoformat(), "price": price}
            )
        result.append(single_prediction)

    return result


def full_fill_real_prices(
    prediction: list[dict[str, Any]], real_prices: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """
    Fills missing real prices in the prediction with None.

    :param prediction: List of dictionaries with 'time' and 'price' keys.
    :param real_prices: List of dictionaries with 'time' and 'price' keys.
    :return: List of dictionaries with filled prices.
    """
    # transform real_prices into a dictionary for fast lookup
    real_prices_dict = {}
    for entry in real_prices:
        real_prices_dict[entry["time"]] = entry["price"]

    # fill missing times and prices in the real_prices_dict
    for entry in prediction:
        if (
            entry["time"] not in real_prices_dict
            or real_prices_dict[entry["time"]] is None
            or np.isnan(real_prices_dict[entry["time"]])
            or not np.isfinite(real_prices_dict[entry["time"]])
        ):
            real_prices_dict[entry["time"]] = np.nan

    real_prices_filled = []
    # recreate the real_prices list of dict sorted by time
    for time in sorted(real_prices_dict.keys()):
        real_prices_filled.append(
            {"time": time, "price": real_prices_dict[time]}
        )

    return real_prices_filled


def get_intersecting_arrays(array1, array2):
    """
    Filters two arrays of dictionaries, keeping only entries that intersect by 'time'.

    :param array1: First array of dictionaries with 'time' and 'price'.
    :param array2: Second array of dictionaries with 'time' and 'price'.
    :return: Two new arrays with only intersecting 'time' values.
    """
    # Extract times from the second array as a set for fast lookup
    times_in_array2 = {entry["time"] for entry in array2}

    # Filter array1 to include only matching times
    filtered_array1 = [
        entry for entry in array1 if entry["time"] in times_in_array2
    ]

    # Extract times from the first array as a set
    times_in_array1 = {entry["time"] for entry in array1}

    # Filter array2 to include only matching times
    filtered_array2 = [
        entry for entry in array2 if entry["time"] in times_in_array1
    ]

    return filtered_array1, filtered_array2


def round_time_to_minutes(
    dt: datetime, in_seconds: int, extra_seconds=0
) -> datetime:
    """round validation time to the closest minute and add extra minutes

    Args:
        dt (datetime): request_time
        in_seconds (int): 60
        extra_seconds (int, optional): self.timeout_extra_seconds: 120. Defaults to 0.

    Returns:
        datetime: rounded-up datetime
    """
    # Define the rounding interval
    rounding_interval = timedelta(seconds=in_seconds)

    # Calculate the number of seconds since the start of the day
    seconds = (
        dt - dt.replace(hour=0, minute=0, second=0, microsecond=0)
    ).total_seconds()

    # Calculate the next multiple of time_increment in seconds
    next_interval_seconds = (
        (seconds // rounding_interval.total_seconds()) + 1
    ) * rounding_interval.total_seconds()

    # Get the rounded-up datetime
    rounded_time = (
        dt.replace(hour=0, minute=0, second=0, microsecond=0)
        + timedelta(seconds=next_interval_seconds)
        + timedelta(seconds=extra_seconds)
    )

    return rounded_time


def from_iso_to_unix_time(iso_time: str):
    # Convert to a datetime object
    dt = datetime.fromisoformat(iso_time).replace(tzinfo=timezone.utc)

    # Convert to Unix time
    return int(dt.timestamp())


def timeout_from_start_time(
    config_timeout: Optional[float], start_time_str: str
) -> float:
    """
    Calculate the timeout duration from the start_time to the current time.

    :param start_time: ISO 8601 string representing the start time.
    :return: Timeout duration in seconds.
    """
    if config_timeout is not None:
        return config_timeout

    # Convert start_time to a datetime object
    start_time = datetime.fromisoformat(start_time_str)

    # Get current date and time
    current_time = datetime.now(timezone.utc)

    # Calculate the timeout duration
    return (start_time - current_time).total_seconds()


def timeout_until(until_time: datetime):
    """
    Calculate the timeout duration from the current time to the until_time.

    :param until_time: datetime object representing the end time.
    :return: Timeout duration in seconds.
    """
    # Get current date and time
    current_time = datetime.now(timezone.utc)

    # Calculate the timeout duration
    wait_time = (until_time - current_time).total_seconds()

    return wait_time if wait_time > 0 else 0


def convert_list_elements_to_str(items: list[int]) -> list[str]:
    return [str(x) for x in items]
