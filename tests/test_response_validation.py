from datetime import datetime
from synth.simulation_input import SimulationInput
from synth.validator.response_validation import validate_responses, CORRECT


start_time = datetime.fromisoformat("2023-01-01T00:00:00")
time_increment = 1


def test_validate_responses_process_time_none():
    simulation_input = SimulationInput(
        start_time=start_time.isoformat(),
        num_simulations=1,
        time_length=10,
        time_increment=time_increment,
    )
    response: list = []
    request_time = datetime.fromisoformat("2023-01-01T00:00:00")
    process_time_str = None

    result = validate_responses(
        response, simulation_input, request_time, process_time_str
    )
    assert result == "time out or internal server error (process time is None)"


def test_validate_responses_received_after_start_time():
    simulation_input = SimulationInput(
        start_time=start_time.isoformat(),
        num_simulations=1,
        time_length=10,
        time_increment=time_increment,
    )
    response: list = []
    request_time = datetime.fromisoformat("2023-01-01T00:00:00")
    process_time_str = "10"

    result = validate_responses(
        response, simulation_input, request_time, process_time_str
    )
    assert (
        result
        == "Response received after the simulation start time: expected 2023-01-01 00:00:00, got 2023-01-01 00:00:10"
    )


def test_validate_responses_empty_response():
    simulation_input = SimulationInput(
        start_time=start_time.isoformat(),
        num_simulations=1,
        time_length=10,
        time_increment=time_increment,
    )
    response: list = []
    request_time = datetime.fromisoformat("2023-01-01T00:00:00")
    process_time_str = "0"

    result = validate_responses(
        response, simulation_input, request_time, process_time_str
    )
    assert result == "Response is empty"


def test_validate_responses_incorrect_type():
    simulation_input = SimulationInput(
        start_time=start_time.isoformat(),
        num_simulations=1,
        time_length=10,
        time_increment=time_increment,
    )
    response: dict = {
        "time": start_time.timestamp(),
        "increment": time_increment,
        "paths": [[123.45] * 11],
    }
    request_time = datetime.fromisoformat("2023-01-01T00:00:00")
    process_time_str = "0"

    result = validate_responses(
        response, simulation_input, request_time, process_time_str
    )
    assert (
        result
        == "Response format is incorrect: expected list, got <class 'dict'>"
    )


def test_validate_responses_incorrect_number_of_paths():
    simulation_input = SimulationInput(
        start_time=start_time.isoformat(),
        num_simulations=2,
        time_length=10,
        time_increment=time_increment,
    )
    response: list = [start_time.timestamp(), time_increment]
    request_time = datetime.fromisoformat("2023-01-01T00:00:00")
    process_time_str = "0"

    result = validate_responses(
        response, simulation_input, request_time, process_time_str
    )
    assert result == "Number of paths is incorrect: expected 2, got 0"

    response: list = [start_time.timestamp(), time_increment, [123.45]]
    request_time = datetime.fromisoformat("2023-01-01T00:00:00")
    process_time_str = "0"

    result = validate_responses(
        response, simulation_input, request_time, process_time_str
    )
    assert result == "Number of paths is incorrect: expected 2, got 1"


def test_validate_responses_incorrect_path_type():
    simulation_input = SimulationInput(
        start_time=start_time.isoformat(),
        num_simulations=2,
        time_length=1,
        time_increment=time_increment,
    )
    response: list = [
        start_time.timestamp(),
        time_increment,
        {"price": 123.45},
        {"price": 123.45},
    ]
    request_time = datetime.fromisoformat("2023-01-01T00:00:00")
    process_time_str = "0"

    result = validate_responses(
        response, simulation_input, request_time, process_time_str
    )
    assert (
        result == "Path format is incorrect: expected list, got <class 'dict'>"
    )


def test_validate_responses_incorrect_number_of_time_points():
    simulation_input = SimulationInput(
        start_time=start_time.isoformat(),
        num_simulations=1,
        time_length=10,
        time_increment=time_increment,
    )
    response: list = [start_time.timestamp(), time_increment, [123.45]]
    request_time = datetime.fromisoformat("2023-01-01T00:00:00")
    process_time_str = "0"

    result = validate_responses(
        response, simulation_input, request_time, process_time_str
    )
    assert result == "Number of time points is incorrect: expected 11, got 1"


def test_validate_responses_incorrect_start_time():
    simulation_input = SimulationInput(
        start_time=start_time.isoformat(),
        num_simulations=1,
        time_length=10,
        time_increment=time_increment,
    )

    response: list = [start_time.isoformat(), time_increment, [123.45] * 11]
    request_time = datetime.fromisoformat("2023-01-01T00:00:00")
    process_time_str = "0"

    result = validate_responses(
        response, simulation_input, request_time, process_time_str
    )
    assert (
        result
        == "Start time format is incorrect: expected int or float, got <class 'str'>"
    )

    response: list = [
        start_time.timestamp() + 1,
        time_increment,
        [123.45] * 11,
    ]
    request_time = datetime.fromisoformat("2023-01-01T00:00:00")
    process_time_str = "0"

    result = validate_responses(
        response, simulation_input, request_time, process_time_str
    )
    assert (
        result
        == "Start time timestamp is incorrect: expected 1672527600, got 1672527601"
    )


def test_validate_responses_incorrect_time_increment():
    simulation_input = SimulationInput(
        start_time=start_time.isoformat(),
        num_simulations=1,
        time_length=10,
        time_increment=time_increment,
    )
    response: list = [start_time.timestamp(), "", [123.45] * 11]
    request_time = datetime.fromisoformat("2023-01-01T00:00:00")
    process_time_str = "0"

    result = validate_responses(
        response, simulation_input, request_time, process_time_str
    )
    assert (
        result
        == "Time increment format is incorrect: expected int, got <class 'str'>"
    )

    response: list = [
        start_time.timestamp(),
        time_increment + 1,
        [123.45] * 11,
    ]
    request_time = datetime.fromisoformat("2023-01-01T00:00:00")
    process_time_str = "0"

    result = validate_responses(
        response, simulation_input, request_time, process_time_str
    )
    assert result == "Time increment is incorrect: expected 1, got 2"


def test_validate_responses_incorrect_price_format():
    simulation_input = SimulationInput(
        start_time=start_time.isoformat(),
        num_simulations=1,
        time_length=10,
        time_increment=time_increment,
    )
    response: list = [start_time.timestamp(), time_increment, ["123.45"] * 11]
    request_time = datetime.fromisoformat("2023-01-01T00:00:00")
    process_time_str = "0"

    result = validate_responses(
        response, simulation_input, request_time, process_time_str
    )
    assert (
        result
        == "Price format is incorrect: expected int or float, got <class 'str'>"
    )


def test_validate_responses_incorrect_price_digits():
    simulation_input = SimulationInput(
        start_time=start_time.isoformat(),
        num_simulations=1,
        time_length=10,
        time_increment=time_increment,
    )
    response: list = [
        start_time.timestamp(),
        time_increment,
        [123.456789] * 11,
    ]
    request_time = datetime.fromisoformat("2023-01-01T00:00:00")
    process_time_str = "0"

    result = validate_responses(
        response, simulation_input, request_time, process_time_str
    )
    assert result == "Price format is incorrect: too many digits 123.456789"


def test_validate_responses_correct():
    simulation_input = SimulationInput(
        start_time=start_time.isoformat(),
        num_simulations=1,
        time_length=3,
        time_increment=time_increment,
    )
    response: list = [start_time.timestamp(), time_increment, [123.45678] * 4]
    request_time = datetime.fromisoformat("2023-01-01T00:00:00")
    process_time_str = "0"

    result = validate_responses(
        response, simulation_input, request_time, process_time_str
    )
    assert result == CORRECT
