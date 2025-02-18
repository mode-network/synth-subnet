from datetime import datetime

import numpy as np
import pytest
from numpy.testing import assert_almost_equal

from synth.db.models import (
    miner_predictions,
    validator_requests,
    miner_scores,
)
from synth.simulation_input import SimulationInput
from synth.validator import response_validation
from synth.validator.forward import remove_zero_rewards
from synth.validator.miner_data_handler import MinerDataHandler
from synth.validator.price_data_provider import PriceDataProvider
from synth.validator.reward import compute_softmax, get_rewards
from tests.utils import generate_values


@pytest.fixture(scope="function", autouse=True)
def setup_data(db_engine):
    with db_engine.connect() as connection:
        with connection.begin():
            ms = miner_scores.delete()
            mp = miner_predictions.delete()
            vr = validator_requests.delete()
            connection.execute(ms)
            connection.execute(mp)
            connection.execute(vr)


def test_compute_softmax_1():
    score_values = np.array([1000, 1500, 2000])
    expected_score = np.array([0.506, 0.307, 0.186])

    actual_score = compute_softmax(score_values, -0.001)

    assert_almost_equal(actual_score, expected_score, decimal=3)


def test_compute_softmax_2():
    score_values = np.array([1000, 1500, 2000, -1])
    expected_score = np.array([0.506, 0.307, 0.186, 0])

    actual_score = compute_softmax(score_values, -0.001)

    assert_almost_equal(actual_score, expected_score, decimal=3)


def test_remove_zero_rewards():
    moving_average_rewards = [
        {
            "miner_uid": 0,
            "smoothed_score": float(0),
            "reward_weight": float(0),
            "updated_at": "2024-11-20T00:00:00",
        },
        {
            "miner_uid": 1,
            "smoothed_score": float(0.2),
            "reward_weight": float(0.2),
            "updated_at": "2024-11-20T00:00:00",
        },
        {
            "miner_uid": 2,
            "smoothed_score": float(0),
            "reward_weight": float(0),
            "updated_at": "2024-11-20T00:00:00",
        },
        {
            "miner_uid": 3,
            "smoothed_score": float(0.8),
            "reward_weight": float(0.8),
            "updated_at": "2024-11-20T00:00:00",
        },
    ]

    filtered_rewards, filtered_miner_uids = remove_zero_rewards(
        moving_average_rewards
    )

    assert len(filtered_miner_uids) == 2
    assert len(filtered_rewards) == 2
    assert filtered_miner_uids[0] == 1
    assert filtered_miner_uids[1] == 3
    assert filtered_rewards[0] == 0.2
    assert filtered_rewards[1] == 0.8


def test_get_rewards(db_engine):
    miner_id = 0
    start_time = "2024-11-26T00:00:00+00:00"
    scored_time = "2024-11-28T00:00:00+00:00"

    simulation_input = SimulationInput(
        asset="BTC",
        start_time=start_time,
        time_increment=300,
        time_length=86400,
        num_simulations=1,
    )

    handler = MinerDataHandler(db_engine)
    price_data_provider = PriceDataProvider(
        "BTC"
    )  # TODO: add a mock instead of the real provider

    values = generate_values(datetime.fromisoformat(start_time))
    simulation_data = {miner_id: (values, response_validation.CORRECT, "1.2")}
    handler.save_responses(simulation_data, simulation_input, datetime.now())

    validator_request_id = handler.get_latest_prediction_request(
        scored_time, simulation_input
    )

    softmax_scores = get_rewards(
        handler,
        price_data_provider,
        SimulationInput(
            asset="BTC",
            start_time=scored_time,
            time_increment=60,  # default: 5 mins
            time_length=3600,  # default: 1 day
            num_simulations=1,  # default: 100
        ),
        [miner_id],  # TODO: add another test with more miners
        validator_request_id,
        softmax_beta=-0.002,
    )

    assert len(softmax_scores) == 2
    assert softmax_scores[1][0]["miner_uid"] == miner_id
    assert softmax_scores[1][0]["softmax_score"] == 1.0
    assert len(softmax_scores[1][0]["crps_data"]) == 72
    # TODO: assert the scores
