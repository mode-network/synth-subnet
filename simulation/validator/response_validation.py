import json
from datetime import datetime, timedelta


from simulation.simulation_input import SimulationInput


def validate_responses(response, simulation_input: SimulationInput) -> bool:
    """
    Validate responses from miners.

    Return False if response is incorrect.
    """
    if response is None or len(response) == 0:
        return False

    # check the number of paths
    if len(response) != simulation_input.num_simulations:
        return False

    for path in response:
        # check the number of time points
        if (
            len(path)
            != (
                simulation_input.time_length // simulation_input.time_increment
            )
            + 1
        ):
            return False

        # check the start time
        if path[0]["time"] != simulation_input.start_time:
            return False

        for i in range(1, len(path)):
            # check the time increment
            i_minus_one_time = datetime.fromisoformat(path[i - 1]["time"])
            i_time = datetime.fromisoformat(path[i]["time"])
            expected_delta = timedelta(seconds=simulation_input.time_increment)
            if i_time - i_minus_one_time != expected_delta:
                return False

            # check the price format
            if not isinstance(path[i]["price"], (int, float)):
                return False

    return True
