# The MIT License (MIT)
# Copyright © 2023 Yuma Rao
# TODO(developer): Set your name
# Copyright © 2023 <your name>

# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
# documentation files (the “Software”), to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
# and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all copies or substantial portions of
# the Software.

from typing import List

# THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
# THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.
import numpy as np

from synth.simulation_input import SimulationInput
from synth.utils.helpers import get_intersecting_arrays
from synth.validator.crps_calculation import calculate_crps_for_miner
from synth.validator.miner_data_handler import MinerDataHandler
from synth.validator.price_data_provider import PriceDataProvider
from synth.validator import response_validation

import bittensor as bt


def reward(
    miner_data_handler: MinerDataHandler,
    price_data_provider: PriceDataProvider,
    miner_uid: int,
    simulation_input: SimulationInput,
    validator_request_id: int,
):
    """
    Reward the miner response to the simulation_input request. This method returns a reward
    value for the miner, which is used to update the miner's score.

    Returns:
    - float: The reward value for the miner.
    """

    miner_prediction_id, predictions, format_validation = (
        miner_data_handler.get_miner_prediction(
            miner_uid, validator_request_id
        )
    )

    if format_validation != response_validation.CORRECT:
        return (
            -1,
            [],
            [],
            miner_prediction_id,
        )  # represents no prediction data from the miner

    # get last time in predictions
    end_time = predictions[0][len(predictions[0]) - 1]["time"]
    real_prices = price_data_provider.fetch_data(end_time)

    if len(real_prices) == 0:
        return -1, [], [], miner_prediction_id

    # in case some of the time points is not overlapped
    intersecting_predictions = []
    intersecting_real_price = real_prices
    for prediction in predictions:
        intersecting_prediction, intersecting_real_price = (
            get_intersecting_arrays(prediction, intersecting_real_price)
        )
        intersecting_predictions.append(intersecting_prediction)

    predictions_path = [
        [entry["price"] for entry in sublist]
        for sublist in intersecting_predictions
    ]
    real_price_path = [entry["price"] for entry in intersecting_real_price]

    try:
        score, detailed_crps_data = calculate_crps_for_miner(
            np.array(predictions_path).astype(float),
            np.array(real_price_path),
            simulation_input.time_increment,
        )
    except Exception as e:
        bt.logging.error(
            f"Error calculating CRPS for miner {miner_uid} with prediction_id {miner_prediction_id}: {e}"
        )
        return -1, [], [], miner_prediction_id

    return score, detailed_crps_data, real_prices, miner_prediction_id


def get_rewards(
    miner_data_handler: MinerDataHandler,
    price_data_provider: PriceDataProvider,
    simulation_input: SimulationInput,
    miner_uids: List[int],
    validator_request_id: int,
) -> tuple[np.ndarray, list]:
    """
    Returns an array of rewards for the given query and responses.

    Args:
    - query (int): The query sent to the miner.
    - responses (List[float]): A list of responses from the miner.

    Returns:
    - np.ndarray: An array of rewards for the given query and responses.
    """

    bt.logging.info(f"In rewards, miner_uids={miner_uids}")

    scores = []
    detailed_crps_data_list = []
    real_prices_list = []
    prediction_id_list = []
    for miner_uid in miner_uids:
        # function that calculates a score for an individual miner
        score, detailed_crps_data, real_prices, miner_prediction_id = reward(
            miner_data_handler,
            price_data_provider,
            miner_uid,
            simulation_input,
            validator_request_id,
        )
        scores.append(score)
        detailed_crps_data_list.append(detailed_crps_data)
        real_prices_list.append(real_prices)
        prediction_id_list.append(miner_prediction_id)

    score_values = np.array(scores)
    prompt_scores_v2, percentile90, lowest_score = compute_prompt_scores_v2(
        score_values
    )

    if prompt_scores_v2 is None:
        return None, []

    # gather all the detailed information
    # for log and debug purposes
    detailed_info = [
        {
            "miner_uid": miner_uid,
            "prompt_score_v2": float(prompt_score_v2),
            "percentile90": float(percentile90),
            "lowest_score": float(lowest_score),
            "miner_prediction_id": miner_prediction_id,
            "total_crps": float(score),
            "crps_data": clean_numpy_in_crps_data(crps_data),
            "real_prices": real_prices,
        }
        for miner_uid, score, crps_data, prompt_score_v2, real_prices, miner_prediction_id in zip(
            miner_uids,
            scores,
            detailed_crps_data_list,
            prompt_scores_v2,
            real_prices_list,
            prediction_id_list,
        )
    ]

    return prompt_scores_v2, detailed_info


def compute_prompt_scores_v2(score_values: np.ndarray):
    if np.all(score_values == -1):
        return None, 0, 0
    score_values_valid = score_values[score_values != -1]
    percentile90 = np.percentile(score_values_valid, 90)
    capped_scores = np.minimum(score_values, percentile90)
    capped_scores = np.where(score_values == -1, percentile90, capped_scores)
    lowest_score = np.min(capped_scores)
    return capped_scores - lowest_score, percentile90, lowest_score


def compute_softmax(score_values: np.ndarray, beta: float) -> np.ndarray:
    bt.logging.info(f"Going to use the following value of beta: {beta}")

    exp_scores = np.exp(beta * score_values)
    softmax_scores_valid = exp_scores / np.sum(exp_scores)
    softmax_scores = softmax_scores_valid

    return softmax_scores


def clean_numpy_in_crps_data(crps_data: list) -> list:
    cleaned_crps_data = [
        {
            key: (float(value) if isinstance(value, np.float64) else value)
            for key, value in item.items()
        }
        for item in crps_data
    ]
    return cleaned_crps_data
