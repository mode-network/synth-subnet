from datetime import datetime, timedelta


from simulation.simulation_input import SimulationInput

CORRECT = "CORRECT"


def validate_responses(response, simulation_input: SimulationInput) -> str:
    """
    Validate responses from miners.

    Return False if response is incorrect.
    """
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
