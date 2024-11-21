# The MIT License (MIT)
# Copyright © 2023 Yuma Rao
# TODO(developer): Set your name
# Copyright © 2023 <your name>
from datetime import datetime, timedelta

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
from typing import List

from simulation.utils.helpers import get_intersecting_arrays
from simulation.validator.crps_calculation import calculate_crps_for_miner
from simulation.simulation_input import SimulationInput
from simulation.validator.miner_data_handler import MinerDataHandler
from simulation.validator.price_data_provider import PriceDataProvider

# Create a single shared instance
provider = PriceDataProvider()


def reward(
        miner_data_handler: MinerDataHandler,
        miner_uid: int,
        simulation_input: SimulationInput,
        real_prices,
        validation_time: str,
    ):
    """
    Reward the miner response to the simulation_input request. This method returns a reward
    value for the miner, which is used to update the miner's score.

    Returns:
    - float: The reward value for the miner.
    """

    predictions = miner_data_handler.get_values(miner_uid, validation_time)

    intersecting_predictions, intersecting_real_price = get_intersecting_arrays(predictions, real_prices)

    predictions_path = [entry["price"] for entry in intersecting_predictions]
    real_price_path = [entry["price"] for entry in intersecting_real_price]

    score = calculate_crps_for_miner(
        miner_uid,
        np.array(predictions_path),
        np.array(real_price_path),
        simulation_input.time_increment
    )

    return score


def get_rewards(
    self,
    miner_data_handler: MinerDataHandler,
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
    # current_price = get_asset_price(simulation_input.asset)
    # time_increment = simulation_input.time_increment
    # time_length = simulation_input.time_length
    # sigma = simulation_input.sigma

    # write our own function
    # think if we are ok with writing our own service that provides the historical prices
    # real_price_path = generate_real_price_path(
    #     current_price, time_increment, time_length, sigma)
    previous_date_time = (datetime.now() - timedelta(days=1)).isoformat()
    real_prices = provider.fetch_data(previous_date_time, validation_time)

    scores = []
    for i, miner_id in enumerate(miner_uids):
        # function that calculates a score for an individual miner
        scores.append(reward(miner_data_handler, miner_id, simulation_input, real_prices))

    score_values = np.array(scores)

    # --- Softmax Normalization ---
    beta = -1 / 1000.0  # Negative beta to give higher weight to lower scores

    # Compute softmax scores
    exp_scores = np.exp(beta * score_values)
    softmax_scores = exp_scores / np.sum(exp_scores)

    return softmax_scores
