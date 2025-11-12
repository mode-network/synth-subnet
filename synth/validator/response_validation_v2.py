from datetime import datetime, timedelta
import typing


from synth.simulation_input import SimulationInput

CORRECT = "CORRECT"


def validate_path(
    path: typing.List[float], expected_time_points: int
) -> typing.Optional[str]:
    if not isinstance(path, list):
        return f"Path format is incorrect: expected list, got {type(path)}"

    # check the number of time points
    if len(path) != expected_time_points:
        return f"Number of time points is incorrect: expected {expected_time_points}, got {len(path)}"

    for point in path:
        # check the price format
        if not isinstance(point, (int, float)):
            return f"Price format is incorrect: expected int or float, got {type(point)}"

        if len(str(point).replace(".", "")) > 8:
            return f"Price format is incorrect: too many digits {point}"

    return None


def validate_response_type(response) -> typing.Optional[str]:
    # check if the response is empty
    if response is None:
        return "Response is empty"

    if not isinstance(response, (tuple, list)):
        return f"Response format is incorrect: expected tuple or list, got {type(response)}"

    if len(response) == 0:
        return "Response is empty"

    if not isinstance(response[0], int):
        return f"Start time format is incorrect: expected int, got {type(response[0])}"

    if not isinstance(response[1], int):
        return f"Time increment format is incorrect: expected int, got {type(response[1])}"


def validate_responses(
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

    error_message = validate_response_type(response)
    if error_message:
        return error_message

    # check the start time
    first_time_timestamp: int = response[0]
    expected_first_time_timestamp = int(start_time.timestamp())
    if first_time_timestamp != expected_first_time_timestamp:
        return f"Start time timestamp is incorrect: expected {expected_first_time_timestamp}, got {first_time_timestamp}"

    # check the time increment
    time_increment: int = response[1]
    expected_time_increment = simulation_input.time_increment
    if time_increment != expected_time_increment:
        return f"Time increment is incorrect: expected {expected_time_increment}, got {time_increment}"

    number_of_paths = len(response[2:])
    # check the number of paths
    if number_of_paths != simulation_input.num_simulations:
        return f"Number of paths is incorrect: expected {simulation_input.num_simulations}, got {number_of_paths}"

    all_paths = response[2:]
    expected_time_points = (
        simulation_input.time_length // simulation_input.time_increment + 1
    )
    for path in all_paths:
        error_message = validate_path(path, expected_time_points)
        if error_message:
            return error_message

    return CORRECT
