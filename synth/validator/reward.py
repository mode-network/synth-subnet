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
    softmax_beta: float,
) -> (np.ndarray, []):
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
    for i, miner_id in enumerate(miner_uids):
        # function that calculates a score for an individual miner
        score, detailed_crps_data, real_prices, miner_prediction_id = reward(
            miner_data_handler,
            price_data_provider,
            miner_id,
            simulation_input,
            validator_request_id,
        )
        scores.append(score)
        detailed_crps_data_list.append(detailed_crps_data)
        real_prices_list.append(real_prices)
        prediction_id_list.append(miner_prediction_id)

    score_values = np.array(scores)
    softmax_scores = compute_softmax(score_values, softmax_beta)


    # apply sigmoid function to the softmax scores
    valid_scores_mask = score_values != -1
    miner_scores = list(zip(miner_uids, score_values))
    valid_miner_scores = [ms for ms, mask in zip(miner_scores, valid_scores_mask) if mask]
    valid_miner_scores.sort(key=lambda x: x[1], reverse=True)
    rank_mapping = {uid: rank + 1 for rank, (uid, _) in enumerate(valid_miner_scores)}
    
    ranks = np.zeros_like(softmax_scores)
    for i, (uid, _) in enumerate(miner_scores):
        if valid_scores_mask[i]:
            ranks[i] = rank_mapping[uid]
    
    n_valid = np.sum(valid_scores_mask)
    normalized_ranks = (ranks - 1) / (n_valid - 1) if n_valid > 1 else ranks
    
    steepness = 10  # mark how sharp the s-curve is
    sigmoid_rewards = 1 / (1 + np.exp(-steepness * (normalized_ranks - 0.5)))

    softmax_scores = sigmoid_rewards


    # gather all the detailed information
    # for log and debug purposes
    detailed_info = [
        {
            "miner_uid": miner_uid,
            "score": score,
            "crps_data": clean_numpy_in_crps_data(crps_data),
            "softmax_score": float(softmax_score),
            "real_prices": real_prices,
            "miner_prediction_id": miner_prediction_id,
        }
        for miner_uid, score, crps_data, softmax_score, real_prices, miner_prediction_id in zip(
            miner_uids,
            scores,
            detailed_crps_data_list,
            softmax_scores,
            real_prices_list,
            prediction_id_list,
        )
    ]

    return softmax_scores, detailed_info


def compute_softmax(score_values: np.ndarray, beta: float) -> np.ndarray:
    # Mask out invalid scores (e.g., -1)
    mask = score_values != -1  # True for values to include in computation

    bt.logging.info(f"Going to use the following value of beta: {beta}")

    # Compute softmax scores only for valid values
    exp_scores = np.exp(beta * score_values[mask])
    softmax_scores_valid = exp_scores / np.sum(exp_scores)

    # Create final softmax_scores with 0 where scores were -1
    softmax_scores = np.zeros_like(score_values, dtype=float)
    softmax_scores[mask] = softmax_scores_valid

    return softmax_scores


def clean_numpy_in_crps_data(crps_data: []) -> []:
    cleaned_crps_data = [
        {
            key: (float(value) if isinstance(value, np.float64) else value)
            for key, value in item.items()
        }
        for item in crps_data
    ]
    return cleaned_crps_data
