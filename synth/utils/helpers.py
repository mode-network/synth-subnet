from typing import Optional
from datetime import datetime, timedelta, timezone


def get_current_time() -> datetime:
    # Get current date and time
    return datetime.now(timezone.utc).replace(microsecond=0)


def round_to_8_significant_digits(num: float) -> float:
    """Round a float to 8 significant digits."""
    if num == 0:
        return 0.0
    from math import log10, floor

    digits = 8
    # calculate the order of magnitude of the number
    magnitude = floor(log10(abs(num)))
    # calculate the decimal places to round to
    decimal_places = digits - magnitude - 1

    return round(num, decimal_places)


def convert_prices_to_time_format(
    prices: list, start_time_str: str, time_increment: int
):
    """
    Convert an array of float numbers (prices) into the expected predictions format.

    :param prices: List of float numbers representing prices.
    :param start_time: ISO 8601 string representing the start time.
    :param time_increment: Time increment in seconds between consecutive prices.
    :return: Tuple containing start time (as Unix timestamp), time increment, and lists of prices.
    """
    start_time = datetime.fromisoformat(start_time_str).replace(
        tzinfo=timezone.utc
    )
    result = [int(start_time.timestamp()), time_increment]

    for price_item in prices:
        single_prediction = []
        for price in price_item:
            single_prediction.append(round_to_8_significant_digits(price))
        result.append(single_prediction)

    return tuple(result)


def adjust_predictions(predictions: list) -> list:
    if not isinstance(predictions, list):
        return None

    if len(predictions) <= 2:
        return None

    first_element = predictions[0]
    if isinstance(first_element, list):
        first_of_first = first_element[0]
        if isinstance(first_of_first, dict):
            # old format, adjust to the new format
            predictions_path = [
                [entry["price"] for entry in sublist]
                for sublist in predictions
            ]
            return predictions_path

    return predictions[2:]


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
