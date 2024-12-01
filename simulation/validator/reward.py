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

from simulation.simulation_input import SimulationInput
from simulation.utils.helpers import get_intersecting_arrays
from simulation.validator.crps_calculation import calculate_crps_for_miner
from simulation.validator.miner_data_handler import MinerDataHandler
from simulation.validator.price_data_provider import PriceDataProvider

import bittensor as bt


def reward(
        miner_data_handler: MinerDataHandler,
        price_data_provider: PriceDataProvider,
        miner_uid: int,
        simulation_input: SimulationInput,
        validation_time: str,
    ):
    """
    Reward the miner response to the simulation_input request. This method returns a reward
    value for the miner, which is used to update the miner's score.

    Returns:
    - float: The reward value for the miner.
    """

    predictions = miner_data_handler.get_values(miner_uid, validation_time)

    if predictions is None or len(predictions) == 0:
        return -1  # represents no prediction data from the miner

    # get last time in predictions
    end_time = predictions[len(predictions) - 1]["time"]
    real_prices = price_data_provider.fetch_data(end_time)

    if len(real_prices) == 0:
        return -1

    # in case some of the time points is not overlapped
    intersecting_predictions, intersecting_real_price = get_intersecting_arrays(predictions, real_prices)

    predictions_path = [entry["price"] for entry in intersecting_predictions]
    real_price_path = [entry["price"] for entry in intersecting_real_price]

    score = calculate_crps_for_miner(
        miner_uid,
        np.array([predictions_path]),  # calculate_crps_for_miner is intended to work with multiple paths
        np.array(real_price_path),
        simulation_input.time_increment
    )

    return score


def get_rewards(
    miner_data_handler: MinerDataHandler,
    price_data_provider: PriceDataProvider,
    simulation_input: SimulationInput,
    miner_uids: List[int],
    validation_time: str,
) -> np.ndarray:
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
    for i, miner_id in enumerate(miner_uids):
        # function that calculates a score for an individual miner
        scores.append(
            reward(
                miner_data_handler,
                price_data_provider,
                miner_id,
                simulation_input,
                validation_time
            )
        )

    score_values = np.array(scores)
    softmax_scores = compute_softmax(score_values)

    return softmax_scores


def compute_softmax(score_values: np.ndarray) -> np.ndarray:
    # Mask out invalid scores (e.g., -1)
    mask = score_values != -1  # True for values to include in computation

    # --- Softmax Normalization ---
    beta = -1 / 1000.0  # Negative beta to give higher weight to lower scores

    # Compute softmax scores only for valid values
    exp_scores = np.exp(beta * score_values[mask])
    softmax_scores_valid = exp_scores / np.sum(exp_scores)

    # Create final softmax_scores with 0 where scores were -1
    softmax_scores = np.zeros_like(score_values, dtype=float)
    softmax_scores[mask] = softmax_scores_valid

    return softmax_scores
