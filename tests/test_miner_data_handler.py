from datetime import datetime

import pytest

from simulation.db.models import miner_predictions, validator_requests
from simulation.validator import response_validation
from simulation.simulation_input import SimulationInput
from simulation.validator.miner_data_handler import MinerDataHandler
from tests.utils import generate_values


@pytest.fixture(scope="function", autouse=True)
def setup_data(db_engine):
    with db_engine.connect() as connection:
        with connection.begin():
            mp = miner_predictions.delete()
            vr = validator_requests.delete()
            connection.execute(mp)
            connection.execute(vr)


def test_get_values_within_range(db_engine):
    """
    Test retrieving values within the valid time range.
    2024-11-20T00:00:00       2024-11-20T23:55:00
             |-------------------------|                       (Prediction range)

                                            2024-11-22T00:00:00
                                                    |-|        (Scored Time)
    """
    miner_id = 1
    start_time = "2024-11-20T00:00:00"
    scored_time = "2024-11-22T00:00:00"
    simulation_input = SimulationInput(
        asset="BTC",
        start_time=start_time,
        time_increment=300,
        time_length=86400,
        num_simulations=1,
    )

    values = generate_values(datetime.fromisoformat(start_time))
    simulation_data = {miner_id: (values, response_validation.CORRECT, "12")}
    handler = MinerDataHandler(db_engine)
    handler.save_responses(simulation_data, simulation_input, datetime.now())

    validator_request_id = handler.get_latest_prediction_request(
        scored_time, simulation_input
    )
    result = handler.get_miner_prediction(miner_id, validator_request_id)

    # get only second element from the result tuple
    # that corresponds to the prediction result
    prediction = result[1]

    assert len(prediction) == 1
    assert len(prediction[0]) == 288
    assert prediction[0][0] == {"time": "2024-11-20T00:00:00", "price": 90000}
    assert prediction[0][287] == {
        "time": "2024-11-20T23:55:00",
        "price": 233500,
    }


def test_get_values_ongoing_range(db_engine):
    """
    Test retrieving values when current_time overlaps with the range.
    2024-11-20T00:00:00       2024-11-20T23:55:00
             |-------------------------|         (Prediction range)

                2024-11-20T12:00:00
                        |-|                      (Scored Time)
    """
    miner_id = 1
    start_time = "2024-11-20T00:00:00"
    scored_time = "2024-11-20T12:00:00"

    simulation_input = SimulationInput(
        asset="BTC",
        start_time=start_time,
        time_increment=300,
        time_length=86400,
        num_simulations=1,
    )

    values = generate_values(datetime.fromisoformat(start_time))
    simulation_data = {miner_id: (values, response_validation.CORRECT, "12")}
    handler = MinerDataHandler(db_engine)
    handler.save_responses(simulation_data, simulation_input, datetime.now())

    validator_request_id = handler.get_latest_prediction_request(
        scored_time, simulation_input
    )
    result = handler.get_miner_prediction(miner_id, validator_request_id)

    # get only second element from the result tuple
    # that corresponds to the prediction result
    prediction = result[1]

    assert len(prediction) == 0


def test_multiple_records_for_same_miner(db_engine):
    """
    Test handling multiple records for the same miner.
    Should take "Prediction range 2" as the latest one

    2024-11-20T00:00:00       2024-11-20T23:55:00
             |-------------------------|                             (Prediction range 1)

                  2024-11-20T12:00:00       2024-11-21T11:55:00
                           |-------------------------|               (Prediction range 2)

                                                  2024-11-21T15:00:00
                                                          |-|        (Current Time)
    """
    miner_id = 1
    start_time_1 = "2024-11-20T00:00:00+00:00"
    start_time_2 = "2024-11-20T12:00:00+00:00"
    scored_time = "2024-11-21T15:00:00+00:00"

    simulation_input_1 = SimulationInput(
        asset="BTC",
        start_time=start_time_1,
        time_increment=300,
        time_length=86400,
        num_simulations=1,
    )

    simulation_input_2 = SimulationInput(
        asset="BTC",
        start_time=start_time_2,
        time_increment=300,
        time_length=86400,
        num_simulations=1,
    )

    handler = MinerDataHandler(db_engine)

    values_1 = generate_values(datetime.fromisoformat(start_time_1))
    simulation_data_1 = {
        miner_id: (values_1, response_validation.CORRECT, "12")
    }
    handler.save_responses(
        simulation_data_1, simulation_input_1, datetime.now()
    )

    values_2 = generate_values(datetime.fromisoformat(start_time_2))
    simulation_data_2 = {
        miner_id: (values_2, response_validation.CORRECT, "12")
    }
    handler.save_responses(
        simulation_data_2, simulation_input_2, datetime.now()
    )

    validator_request_id = handler.get_latest_prediction_request(
        scored_time, simulation_input_1
    )
    result = handler.get_miner_prediction(miner_id, validator_request_id)

    # get only second element from the result tuple
    # that corresponds to the prediction result
    prediction = result[1]

    assert len(prediction) == 1
    assert len(prediction[0]) == 288
    assert prediction[0][0] == {
        "time": "2024-11-20T12:00:00+00:00",
        "price": 90000,
    }
    assert prediction[0][287] == {
        "time": "2024-11-21T11:55:00+00:00",
        "price": 233500,
    }


def test_multiple_records_for_same_miner_with_overlapping(db_engine):
    """
    Test handling multiple records for the same miner with overlapping records.
    Should take "Prediction range 1" as the latest one

    2024-11-20T00:00:00       2024-11-20T23:55:00
             |-------------------------|                             (Prediction range 1)

                  2024-11-20T12:00:00       2024-11-21T11:55:00
                           |-------------------------|               (Prediction range 2)

                                    2024-11-21T03:00:00
                                            |-|                      (Scored Time)
    """
    miner_id = 1
    start_time_1 = "2024-11-20T00:00:00+00:00"
    start_time_2 = "2024-11-20T12:00:00+00:00"
    scored_time = "2024-11-21T03:00:00+00:00"

    simulation_input_1 = SimulationInput(
        asset="BTC",
        start_time=start_time_1,
        time_increment=300,
        time_length=86400,
        num_simulations=1,
    )

    simulation_input_2 = SimulationInput(
        asset="BTC",
        start_time=start_time_2,
        time_increment=300,
        time_length=86400,
        num_simulations=1,
    )

    handler = MinerDataHandler(db_engine)

    values_1 = generate_values(datetime.fromisoformat(start_time_1))
    simulation_data_1 = {
        miner_id: (values_1, response_validation.CORRECT, "12")
    }
    handler.save_responses(
        simulation_data_1, simulation_input_1, datetime.now()
    )

    values_2 = generate_values(datetime.fromisoformat(start_time_2))
    simulation_data_2 = {
        miner_id: (values_2, response_validation.CORRECT, "12")
    }
    handler.save_responses(
        simulation_data_2, simulation_input_2, datetime.now()
    )

    validator_request_id = handler.get_latest_prediction_request(
        scored_time, simulation_input_1
    )
    result = handler.get_miner_prediction(miner_id, validator_request_id)

    # get only second element from the result tuple
    # that corresponds to the prediction result
    prediction = result[1]

    assert len(prediction) == 1
    assert len(prediction[0]) == 288
    assert prediction[0][0] == {
        "time": "2024-11-20T00:00:00+00:00",
        "price": 90000,
    }
    assert prediction[0][287] == {
        "time": "2024-11-20T23:55:00+00:00",
        "price": 233500,
    }


def test_no_data_for_miner(db_engine):
    """Test retrieving values for a miner that doesn't exist."""
    scored_time = "2024-11-20T12:00:00+00:00"

    simulation_input = SimulationInput(
        asset="BTC",
        start_time=scored_time,
        time_increment=300,
        time_length=86400,
        num_simulations=1,
    )

    handler = MinerDataHandler(db_engine)

    validator_request_id = handler.get_latest_prediction_request(
        scored_time, simulation_input
    )
    assert validator_request_id is None


def test_get_values_incorrect_format(db_engine):
    """
    Test retrieving values within the valid time range.
    2024-11-20T00:00:00       2024-11-20T23:55:00
             |-------------------------|                       (Prediction range)

                                            2024-11-22T00:00:00
                                                    |-|        (Scored Time)
    """
    miner_id = 1
    start_time = "2024-11-20T00:00:00"
    scored_time = "2024-11-22T00:00:00"
    simulation_input = SimulationInput(
        asset="BTC",
        start_time=start_time,
        time_increment=300,
        time_length=86400,
        num_simulations=1,
    )

    error_string = "some errors in the format"
    simulation_data = {miner_id: ([], error_string, "12")}
    handler = MinerDataHandler(db_engine)
    handler.save_responses(simulation_data, simulation_input, datetime.now())

    validator_request_id = handler.get_latest_prediction_request(
        scored_time, simulation_input
    )
    result = handler.get_miner_prediction(miner_id, validator_request_id)

    prediction = result[1]
    format_validation = result[2]

    assert len(prediction) == 0
    assert format_validation == error_string
