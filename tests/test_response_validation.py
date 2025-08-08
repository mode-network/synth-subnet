from datetime import datetime
from synth.simulation_input import SimulationInput
from synth.validator.response_validation import validate_responses, CORRECT


def test_validate_responses_process_time_none():
    simulation_input = SimulationInput(
        start_time="2023-01-01T00:00:00",
        num_simulations=1,
        time_length=10,
        time_increment=1,
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
        start_time="2023-01-01T00:00:00",
        num_simulations=1,
        time_length=10,
        time_increment=1,
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
        start_time="2023-01-01T00:00:00",
        num_simulations=1,
        time_length=10,
        time_increment=1,
    )
    response: list = []
    request_time = datetime.fromisoformat("2023-01-01T00:00:00")
    process_time_str = "0"

    result = validate_responses(
        response, simulation_input, request_time, process_time_str
    )
    assert result == "Response is empty"


def test_validate_responses_incorrect_number_of_paths():
    simulation_input = SimulationInput(
        start_time="2023-01-01T00:00:00",
        num_simulations=2,
        time_length=10,
        time_increment=1,
    )
    response: list = [[]]
    request_time = datetime.fromisoformat("2023-01-01T00:00:00")
    process_time_str = "0"

    result = validate_responses(
        response, simulation_input, request_time, process_time_str
    )
    assert result == "Number of paths is incorrect: expected 2, got 1"


def test_validate_responses_incorrect_number_of_time_points():
    simulation_input = SimulationInput(
        start_time="2023-01-01T00:00:00",
        num_simulations=1,
        time_length=10,
        time_increment=1,
    )
    response: list = [[{"time": "2023-01-01T00:00:00"}]]
    request_time = datetime.fromisoformat("2023-01-01T00:00:00")
    process_time_str = "0"

    result = validate_responses(
        response, simulation_input, request_time, process_time_str
    )
    assert result == "Number of time points is incorrect: expected 11, got 1"


def test_validate_responses_incorrect_start_time():
    simulation_input = SimulationInput(
        start_time="2023-01-01T00:00:00",
        num_simulations=1,
        time_length=10,
        time_increment=1,
    )
    response: list = [[{"time": "2023-01-01T00:00:01"} for _ in range(11)]]
    request_time = datetime.fromisoformat("2023-01-01T00:00:00")
    process_time_str = "0"

    result = validate_responses(
        response, simulation_input, request_time, process_time_str
    )
    assert (
        result
        == "Start time is incorrect: expected 2023-01-01T00:00:00, got 2023-01-01T00:00:01"
    )


def test_validate_responses_incorrect_time_increment():
    simulation_input = SimulationInput(
        start_time="2023-01-01T00:00:00",
        num_simulations=1,
        time_length=10,
        time_increment=1,
    )
    response: list = [
        [{"time": "2023-01-01T00:00:00"}, {"time": "2023-01-01T00:00:02"}]
        + [{"time": "2023-01-01T00:00:00"} for _ in range(9)]
    ]
    request_time = datetime.fromisoformat("2023-01-01T00:00:00")
    process_time_str = "0"

    result = validate_responses(
        response, simulation_input, request_time, process_time_str
    )
    assert (
        result == "Time increment is incorrect: expected 0:00:01, got 0:00:02"
    )


def test_validate_responses_incorrect_price_format():
    simulation_input = SimulationInput(
        start_time="2023-01-01T00:00:00",
        num_simulations=1,
        time_length=10,
        time_increment=1,
    )
    response: list = [
        [{"time": "2023-01-01T00:00:00", "price": 100}]
        + [{"time": "2023-01-01T00:00:01", "price": "100"} for _ in range(10)]
    ]
    request_time = datetime.fromisoformat("2023-01-01T00:00:00")
    process_time_str = "0"

    result = validate_responses(
        response, simulation_input, request_time, process_time_str
    )
    assert (
        result
        == "Price format is incorrect: expected int or float, got <class 'str'>"
    )


def test_validate_responses_missing_time():
    simulation_input = SimulationInput(
        start_time="2023-01-01T00:00:00",
        num_simulations=1,
        time_length=10,
        time_increment=1,
    )
    response: list = [[{"price": 100}] + [{"price": "100"} for _ in range(10)]]
    request_time = datetime.fromisoformat("2023-01-01T00:00:00")
    process_time_str = "0"

    result = validate_responses(
        response, simulation_input, request_time, process_time_str
    )
    assert (
        result == "Start time is incorrect: expected 2023-01-01T00:00:00, got "
    )


def test_validate_responses_missing_price():
    simulation_input = SimulationInput(
        start_time="2023-01-01T00:00:00",
        num_simulations=1,
        time_length=10,
        time_increment=1,
    )
    response: list = [
        [{"time": "2023-01-01T00:00:00"}]
        + [{"time": "2023-01-01T00:00:01"} for _ in range(10)]
    ]
    request_time = datetime.fromisoformat("2023-01-01T00:00:00")
    process_time_str = "0"

    result = validate_responses(
        response, simulation_input, request_time, process_time_str
    )
    assert (
        result
        == "Price format is incorrect: expected int or float, got <class 'NoneType'>"
    )


def test_validate_responses_incorrect_time_type():
    simulation_input = SimulationInput(
        start_time="2023-01-01T00:00:00",
        num_simulations=1,
        time_length=10,
        time_increment=1,
    )
    response: list = [
        [{"time": "2023-01-01T00:00:00", "price": 100}]
        + [{"time": 12456, "price": "100"} for _ in range(10)]
    ]
    request_time = datetime.fromisoformat("2023-01-01T00:00:00")
    process_time_str = "0"

    result = validate_responses(
        response, simulation_input, request_time, process_time_str
    )
    assert (
        result == "Time format is incorrect: expected str, got <class 'int'>"
    )


def test_validate_responses_incorrect_time_format():
    simulation_input = SimulationInput(
        start_time="2023-01-01T00:00:00",
        num_simulations=1,
        time_length=10,
        time_increment=1,
    )
    response: list = [
        [{"time": "2023-01-01T00:00:00", "price": 100}]
        + [{"time": "2023-01-01 0000", "price": "100"} for _ in range(10)]
    ]
    request_time = datetime.fromisoformat("2023-01-01T00:00:00")
    process_time_str = "0"

    result = validate_responses(
        response, simulation_input, request_time, process_time_str
    )
    assert (
        result
        == "Time format is incorrect: expected isoformat, got 2023-01-01 0000"
    )


def test_validate_responses_correct():
    simulation_input = SimulationInput(
        start_time="2023-01-01T00:00:00",
        num_simulations=1,
        time_length=3,
        time_increment=1,
    )
    response: list = [
        [{"time": f"2023-01-01T00:00:0{i}", "price": 100} for i in range(4)]
    ]
    request_time = datetime.fromisoformat("2023-01-01T00:00:00")
    process_time_str = "0"

    result = validate_responses(
        response, simulation_input, request_time, process_time_str
    )
    assert result == CORRECT
