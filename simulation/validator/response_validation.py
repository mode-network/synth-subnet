from datetime import datetime, timedelta
import typing


from simulation.simulation_input import SimulationInput

CORRECT = "CORRECT"


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
        return "time out (process time is None)"

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
        if path[0]["time"] != simulation_input.start_time:
            return f"Start time is incorrect: expected {simulation_input.start_time}, got {path[0]['time']}"

        for i in range(1, len(path)):
            # check the time increment
            i_minus_one_time = datetime.fromisoformat(path[i - 1]["time"])
            i_time = datetime.fromisoformat(path[i]["time"])
            expected_delta = timedelta(seconds=simulation_input.time_increment)
            actual_delta = i_time - i_minus_one_time
            if actual_delta != expected_delta:
                return f"Time increment is incorrect: expected {expected_delta}, got {actual_delta}"

            # check the price format
            if not isinstance(path[i]["price"], (int, float)):
                return f"Price format is incorrect: expected int or float, got {type(path[i]['price'])}"

    return CORRECT
