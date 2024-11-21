from datetime import datetime, timedelta


def get_current_time():
    # Get current date and time
    current_time = datetime.now()
    return current_time


def convert_prices_to_time_format(prices, start_time, time_increment):
    """
    Convert an array of float numbers (prices) into an array of dictionaries with 'time' and 'price'.

    :param prices: List of float numbers representing prices.
    :param start_time: ISO 8601 string representing the start time.
    :param time_increment: Time increment in seconds between consecutive prices.
    :return: List of dictionaries with 'time' and 'price' keys.
    """
    start_time = datetime.fromisoformat(start_time)  # Convert start_time to a datetime object
    result = []

    for i, price in enumerate(prices):
        time_point = start_time + timedelta(seconds=i * time_increment)
        result.append({
            "time": time_point.isoformat(),
            "price": price
        })

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
    filtered_array1 = [entry for entry in array1 if entry["time"] in times_in_array2]

    # Extract times from the first array as a set
    times_in_array1 = {entry["time"] for entry in array1}

    # Filter array2 to include only matching times
    filtered_array2 = [entry for entry in array2 if entry["time"] in times_in_array1]

    return filtered_array1, filtered_array2
