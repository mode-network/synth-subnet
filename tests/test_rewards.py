import numpy as np
import pytest
from numpy.testing import assert_almost_equal

from synth.db.models import (
    miner_predictions,
    validator_requests,
    miner_scores,
)
from synth.validator.forward import remove_zero_rewards
from synth.validator.price_data_provider import PriceDataProvider
from synth.validator.reward import (
    compute_prompt_scores_v2,
    compute_softmax,
    get_rewards,
)
from tests.utils import prepare_random_predictions


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
    expected_score = np.array([0.213, 0.129, 0.078, 0.58])

    actual_score = compute_softmax(score_values, -0.001)

    assert_almost_equal(actual_score, expected_score, decimal=3)


def test_compute_prompt_scores_v2():
    crps_scores = np.array([1000, 1500, 2000, -1])
    expected_prompt_scores = np.array([0, 500, 900, 900])

    actual_score, percentile90, lowest_score = compute_prompt_scores_v2(
        crps_scores
    )

    assert percentile90 == 1900.0
    assert lowest_score == 1000
    assert np.array_equal(actual_score, expected_prompt_scores)


def test_compute_prompt_scores_v2_only_one_miner():
    crps_scores = np.array([1000, -1, -1, -1])
    expected_prompt_scores = np.array(
        [0, 0, 0, 0]
    )  # TODO: not ideal but it's the current behavior

    actual_score, percentile90, lowest_score = compute_prompt_scores_v2(
        crps_scores
    )

    assert percentile90 == 1000
    assert lowest_score == 1000
    assert np.array_equal(actual_score, expected_prompt_scores)


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
    start_time = "2024-11-25T23:58:00+00:00"
    scored_time = "2024-11-28T00:00:00+00:00"

    handler, simulation_input, miner_uids = prepare_random_predictions(
        db_engine, start_time
    )

    price_data_provider = PriceDataProvider("BTC")

    validator_request = handler.get_latest_prediction_request(
        scored_time, simulation_input
    )

    prompt_scores_v2, detailed_info = get_rewards(
        handler,
        price_data_provider,
        validator_request,
    )

    assert prompt_scores_v2 is not None

    percentile90 = detailed_info[0]["percentile90"]

    # find the lowest crps value
    crps_values = [item["total_crps"] for item in detailed_info]
    lowest_crps = float("inf")
    for crps in crps_values:
        # -1 is an invalid prediction
        if crps < lowest_crps and crps != -1:
            lowest_crps = crps

    assert len(prompt_scores_v2) == len(miner_uids)
    assert min(prompt_scores_v2) == 0

    # the max score is the percentile90 - lowest_crps
    assert max(prompt_scores_v2) == percentile90 - lowest_crps

    assert detailed_info[0]["miner_uid"] == miner_uids[0]
    assert len(detailed_info[0]["crps_data"]) == 350
