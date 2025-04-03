from datetime import datetime

import pytest
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from synth.db.models import (
    miner_predictions,
    validator_requests,
    miners as miners_model,
)
from synth.validator import response_validation
from synth.simulation_input import SimulationInput
from synth.validator.miner_data_handler import MinerDataHandler
from synth.validator.price_data_provider import PriceDataProvider
from synth.validator.reward import get_rewards
from tests.utils import generate_values, prepare_random_predictions


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
    miner_uids = [10]
    with db_engine.connect() as connection:
        with connection.begin():
            insert_stmt_validator = insert(miners_model).values(
                [{"miner_uid": uid} for uid in miner_uids]
            )
            connection.execute(insert_stmt_validator)

    miner_uid = miner_uids[0]
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
    simulation_data = {miner_uid: (values, response_validation.CORRECT, "12")}
    handler = MinerDataHandler(db_engine)
    handler.save_responses(simulation_data, simulation_input, datetime.now())

    validator_request = handler.get_latest_prediction_request(
        scored_time, simulation_input
    )
    result = handler.get_miner_prediction(miner_uid, validator_request.id)

    # get only second element from the result tuple
    # that corresponds to the prediction result
    prediction = result.prediction

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
    miner_uids = [10]
    with db_engine.connect() as connection:
        with connection.begin():
            insert_stmt_validator = insert(miners_model).values(
                [{"miner_uid": uid} for uid in miner_uids]
            )
            connection.execute(insert_stmt_validator)

    miner_uid = miner_uids[0]
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
    simulation_data = {miner_uid: (values, response_validation.CORRECT, "12")}
    handler = MinerDataHandler(db_engine)
    handler.save_responses(simulation_data, simulation_input, datetime.now())

    validator_request = handler.get_latest_prediction_request(
        scored_time, simulation_input
    )

    assert validator_request is None


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
    miner_uids = [10]
    with db_engine.connect() as connection:
        with connection.begin():
            insert_stmt_validator = insert(miners_model).values(
                [{"miner_uid": uid} for uid in miner_uids]
            )
            connection.execute(insert_stmt_validator)

    miner_uid = miner_uids[0]
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
        miner_uid: (values_1, response_validation.CORRECT, "12")
    }
    handler.save_responses(
        simulation_data_1, simulation_input_1, datetime.now()
    )

    values_2 = generate_values(datetime.fromisoformat(start_time_2))
    simulation_data_2 = {
        miner_uid: (values_2, response_validation.CORRECT, "12")
    }
    handler.save_responses(
        simulation_data_2, simulation_input_2, datetime.now()
    )

    validator_request = handler.get_latest_prediction_request(
        scored_time, simulation_input_1
    )
    result = handler.get_miner_prediction(miner_uid, validator_request.id)

    # get only second element from the result tuple
    # that corresponds to the prediction result
    prediction = result.prediction

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
    miner_uids = [10]
    with db_engine.connect() as connection:
        with connection.begin():
            insert_stmt_validator = insert(miners_model).values(
                [{"miner_uid": uid} for uid in miner_uids]
            )
            connection.execute(insert_stmt_validator)

    miner_uid = miner_uids[0]
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
        miner_uid: (values_1, response_validation.CORRECT, "12")
    }
    handler.save_responses(
        simulation_data_1, simulation_input_1, datetime.now()
    )

    values_2 = generate_values(datetime.fromisoformat(start_time_2))
    simulation_data_2 = {
        miner_uid: (values_2, response_validation.CORRECT, "12")
    }
    handler.save_responses(
        simulation_data_2, simulation_input_2, datetime.now()
    )

    validator_request = handler.get_latest_prediction_request(
        scored_time, simulation_input_1
    )
    result = handler.get_miner_prediction(miner_uid, validator_request.id)

    # get only second element from the result tuple
    # that corresponds to the prediction result
    prediction = result.prediction

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

    validator_request = handler.get_latest_prediction_request(
        scored_time, simulation_input
    )
    assert validator_request is None


def test_get_values_incorrect_format(db_engine):
    """
    Test retrieving values within the valid time range.
    2024-11-20T00:00:00       2024-11-20T23:55:00
             |-------------------------|                       (Prediction range)

                                            2024-11-22T00:00:00
                                                    |-|        (Scored Time)
    """
    miner_uids = [10]
    with db_engine.connect() as connection:
        with connection.begin():
            insert_stmt_validator = insert(miners_model).values(
                [{"miner_uid": uid} for uid in miner_uids]
            )
            connection.execute(insert_stmt_validator)

    miner_uid = miner_uids[0]
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
    simulation_data = {miner_uid: ([], error_string, "12")}
    handler = MinerDataHandler(db_engine)
    handler.save_responses(simulation_data, simulation_input, datetime.now())

    validator_request = handler.get_latest_prediction_request(
        scored_time, simulation_input
    )
    result = handler.get_miner_prediction(miner_uid, validator_request.id)

    prediction = result.prediction
    format_validation = result.format_validation

    assert len(prediction) == 0
    assert format_validation == error_string


def test_set_get_scores(db_engine):
    handler = MinerDataHandler(db_engine)
    price_data_provider = PriceDataProvider("BTC")
    start_time = "2024-11-25T23:58:00+00:00"
    scored_time = "2024-11-27T00:00:00+00:00"
    handler, simulation_input, _ = prepare_random_predictions(
        db_engine, start_time
    )

    validator_request = handler.get_latest_prediction_request(
        scored_time, simulation_input
    )

    prompt_scores_v2, detailed_info = get_rewards(
        handler,
        price_data_provider,
        validator_request,
    )

    assert prompt_scores_v2 is not None

    handler.set_miner_scores(
        reward_details=detailed_info, scored_time=scored_time
    )

    miner_scores_df = handler.get_miner_scores(
        scored_time_str=scored_time,
        cutoff_days=4,
    )

    print("miner_scores_df", miner_scores_df)


def test_insert_new_miners(db_engine):
    handler = MinerDataHandler(db_engine)

    with db_engine.connect() as connection:
        with connection.begin():
            initial_len = len(
                connection.execute(select(miners_model)).fetchall()
            )

    handler.insert_new_miners(
        [{"neuron_uid": 111, "coldkey": "coldkey111", "hotkey": "hotkey111"}]
    )

    with db_engine.connect() as connection:
        with connection.begin():
            assert (
                len(connection.execute(select(miners_model)).fetchall())
                == initial_len + 1
            )

    handler.insert_new_miners(
        [{"neuron_uid": 111, "coldkey": "coldkey111", "hotkey": "hotkey111"}]
    )

    # Should not insert the same miner again
    with db_engine.connect() as connection:
        with connection.begin():
            assert (
                len(connection.execute(select(miners_model)).fetchall())
                == initial_len + 1
            )

    handler.insert_new_miners(
        [
            {
                "neuron_uid": 111,
                "coldkey": "coldkey111-changed",
                "hotkey": "hotkey111-changed",
            }
        ]
    )

    # Should insert new miner with updated values
    with db_engine.connect() as connection:
        with connection.begin():
            assert (
                len(connection.execute(select(miners_model)).fetchall())
                == initial_len + 2
            )
