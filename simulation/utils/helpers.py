from datetime import datetime, timedelta, timezone


def get_current_time():
    # Get current date and time
    current_time = datetime.now(timezone.utc).replace(microsecond=0)
    return current_time.isoformat()


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


def round_time_to_minutes(dt_str, in_seconds, extra_seconds=0):
    # Convert string to datetime object
    dt = datetime.fromisoformat(dt_str)

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

    return rounded_time.isoformat()


def from_iso_to_unix_time(iso_time):
    # Convert to a datetime object
    dt = datetime.fromisoformat(iso_time).replace(tzinfo=timezone.utc)

    # Convert to Unix time
    unix_time = int(dt.timestamp())

    return unix_time


def timeout_from_start_time(
    config_timeout: float, start_time_str: str
) -> float:
    """
    Calculate the timeout duration from the start_time to the current time.

    :param start_time: ISO 8601 string representing the start time.
    :return: Timeout duration in seconds.
    """
    # Convert start_time to a datetime object
    start_time = datetime.fromisoformat(start_time_str)

    # Get current date and time
    current_time = datetime.now(timezone.utc)

    # Calculate the timeout duration
    dynamic_timeout_duration = (start_time - current_time).total_seconds()
    timeout_duration = max(dynamic_timeout_duration, config_timeout)

    return timeout_duration
