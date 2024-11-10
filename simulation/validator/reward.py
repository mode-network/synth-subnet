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

# THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
# THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.
import numpy as np
from typing import List, Any
import bittensor as bt

from crps_calculation import calculate_crps_for_miner
from simulation.simulation_input import SimulationInput
from simulation.simulations.price_simulation import get_asset_price, generate_real_price_path


def reward(response: np.ndarray[Any, np.dtype], miner_uid: int, simulation_input: SimulationInput, real_price_path):
    """
    Reward the miner response to the simulation_input request. This method returns a reward
    value for the miner, which is used to update the miner's score.

    Returns:
    - float: The reward value for the miner.
    """

    score = calculate_crps_for_miner(
        miner_uid,
        response,
        real_price_path,
        simulation_input.time_increment,
        simulation_input.time_length
    )

    return score


def get_rewards(
    self,
    responses: List[np.ndarray[Any, np.dtype]],
    simulation_input: SimulationInput,
    miner_uids: List[int]
) -> np.ndarray:
    """
    Returns an array of rewards for the given query and responses.

    Args:
    - query (int): The query sent to the miner.
    - responses (List[float]): A list of responses from the miner.

    Returns:
    - np.ndarray: An array of rewards for the given query and responses.
    """
    current_price = get_asset_price(simulation_input.asset)
    time_increment = simulation_input.time_increment
    time_length = simulation_input.time_length
    sigma = simulation_input.sigma

    real_price_path = generate_real_price_path(
        current_price, time_increment, time_length, sigma)

    scores = []
    for i, response in enumerate(responses):
        scores.append(reward(response, miner_uids[i], simulation_input, real_price_path))

    score_values = np.array(scores)

    # --- Softmax Normalization ---
    beta = -1 / 1000.0  # Negative beta to give higher weight to lower scores

    # Compute softmax scores
    exp_scores = np.exp(beta * score_values)
    softmax_scores = exp_scores / np.sum(exp_scores)

    return softmax_scores
