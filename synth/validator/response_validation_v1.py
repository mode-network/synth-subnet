from datetime import datetime, timedelta
import typing


from synth.simulation_input import SimulationInput

CORRECT = "CORRECT"


def datetime_valid(dt_str) -> bool:
    try:
        datetime.fromisoformat(dt_str)
    except ValueError:
        return False
    return True


def validate_datetime(
    dt_str,
) -> typing.Tuple[datetime, typing.Optional[str]]:
    if not isinstance(dt_str, str):
        return (
            datetime.now(),
            f"Time format is incorrect: expected str, got {type(dt_str)}",
        )
    if not datetime_valid(dt_str):
        return (
            datetime.now(),
            f"Time format is incorrect: expected isoformat, got {dt_str}",
        )

    return datetime.fromisoformat(dt_str), None


def validate_responses(  # noqa: C901
    response,
    simulation_input: SimulationInput,
    request_time: datetime,
    process_time_str: typing.Optional[str],
) -> str:
    """
    Validate responses from miners.

    Return a string with the error message
    if the response is not following the expected format or the response is empty,
    otherwise, return "CORRECT".
    """
    # check the process time
    if process_time_str is None:
        return "time out or internal server error (process time is None)"

    received_at = request_time + timedelta(seconds=float(process_time_str))
    start_time = datetime.fromisoformat(simulation_input.start_time)
    if received_at > start_time:
        return f"Response received after the simulation start time: expected {start_time}, got {received_at}"

    # check if the response is empty
    if response is None or len(response) == 0:
        return "Response is empty"

    # check the number of paths
    if len(response) != simulation_input.num_simulations:
        return f"Number of paths is incorrect: expected {simulation_input.num_simulations}, got {len(response)}"

    for path in response:
        # check the number of time points
        expected_time_points = (
            simulation_input.time_length // simulation_input.time_increment + 1
        )
        if len(path) != expected_time_points:
            return f"Number of time points is incorrect: expected {expected_time_points}, got {len(path)}"

        # check the start time
        first_time = path[0].get("time", "")
        if first_time != simulation_input.start_time:
            return f"Start time is incorrect: expected {simulation_input.start_time}, got {first_time}"

        for i in range(1, len(path)):
            # check the time formats
            i_minus_one_str_time = path[i - 1].get("time", "")
            i_minus_one_datetime, error_message = validate_datetime(
                i_minus_one_str_time
            )
            if error_message:
                return error_message

            i_str_time = path[i].get("time", "")
            i_datetime, error_message = validate_datetime(i_str_time)
            if error_message:
                return error_message

            # check the time increment
            expected_delta = timedelta(seconds=simulation_input.time_increment)
            actual_delta = i_datetime - i_minus_one_datetime
            if actual_delta != expected_delta:
                return f"Time increment is incorrect: expected {expected_delta}, got {actual_delta}"

            # check the price format
            price = path[i].get("price")
            if not isinstance(price, (int, float)):
                return f"Price format is incorrect: expected int or float, got {type(price)}"

    return CORRECT
