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

import time
from datetime import datetime

import bittensor as bt
import numpy as np

from simulation.base.validator import BaseValidatorNeuron
from simulation.protocol import Simulation
from simulation.simulation_input import SimulationInput
from simulation.utils.helpers import get_current_time, round_time_to_minutes
from simulation.utils.uids import check_uid_availability
from simulation.validator.miner_data_handler import MinerDataHandler
from simulation.validator.moving_average import compute_weighted_averages
from simulation.validator.price_data_provider import PriceDataProvider
from simulation.validator.reward import get_rewards


async def forward(
    self: BaseValidatorNeuron,
    miner_data_handler: MinerDataHandler,
    price_data_provider: PriceDataProvider,
):
    """
    The forward function is called by the validator every time step.

    It is responsible for querying the network and scoring the responses.

    Args:
        self (:obj:`bittensor.neuron.Neuron`): The neuron object which contains all the necessary state for the validator.
        miner_data_handler (:obj:`simulation.validator.MinerDataHandler`): The MinerDataHandler object which contains all the necessary state for the validator.
        price_data_provider (:obj:`simulation.validator.PriceDataProvider`): The PriceDataProvider returns real prices data for a specific token.

    """
    # TODO(developer): Define how the validator selects a miner to query, how often, etc.
    # get_random_uids is an example method, but you can replace it with your own.
    # miner_uids = get_random_uids(self, k=self.config.neuron.sample_size)

    # getting current validation time
    current_time = get_current_time()

    # round validation time to the closest minute and add 1 extra minute
    start_time = round_time_to_minutes(current_time, 60, 60)

    miner_uids = []
    metagraph_info = []
    for uid in range(len(self.metagraph.S)):
        uid_is_available = check_uid_availability(
            self.metagraph, uid, self.config.neuron.vpermit_tao_limit
        )
        if uid_is_available:
            metagraph_item = {
                "neuron_uid": uid,
                "incentive": float(self.metagraph.I[uid]),
                "rank": float(self.metagraph.R[uid]),
                "stake": float(self.metagraph.S[uid]),
                "trust": float(self.metagraph.T[uid]),
                "emission": float(self.metagraph.E[uid]),
                "coldkey": self.metagraph.coldkeys[uid],
                "hotkey": self.metagraph.hotkeys[uid],
                "updated_at": start_time,
            }
            miner_uids.append(uid)
            metagraph_info.append(metagraph_item)

    miner_data_handler.update_metagraph_history(metagraph_info)

    # input data
    # give me prediction of BTC price for the next 1 day for every 5 min of time
    simulation_input = SimulationInput(
        asset="BTC",
        start_time=start_time,
        time_increment=300,
        time_length=86400,
        num_simulations=100,
    )

    # synapse - is a message that validator sends to miner to get results, i.e. simulation_input in our case
    # Simulation - is our protocol, i.e. input and output message of a miner (application that returns prediction of
    # prices for a chosen asset)
    synapse = Simulation(simulation_input=simulation_input)

    # The dendrite client queries the network:
    # it is the actual call to all the miners from validator
    # returns an array of responses (predictions) for each of the miners
    # ======================================================
    # miner has a unique uuid in the subnetwork
    # ======================================================
    # axon is a server application that accepts requests on the miner side
    # ======================================================
    responses = await self.dendrite(
        # Send the query to selected miner axons in the network.
        axons=[self.metagraph.axons[uid] for uid in miner_uids],
        # Construct a synapse object. This contains a simulation input parameters.
        synapse=synapse,
        # All responses have the deserialize function called on them before returning.
        # You are encouraged to define your own deserialization function.
        # ======================================================
        # we are using numpy for the outputs now - do we need to write a function that deserializes from json to numpy?
        # you can find that function in "simulation.protocol.Simulation"
        deserialize=True,
    )

    # Log the results for monitoring purposes.
    # bt.logging.info(f"Received responses: {responses}")

    miner_predictions = {}
    for i, response in enumerate(responses):
        if response is None or len(response) == 0:
            continue
        miner_id = miner_uids[i]
        miner_predictions[miner_id] = response

    miner_data_handler.save_responses(miner_predictions, simulation_input)

    # scored_time is the same as start_time for a single validator step
    # but the meaning is different
    # start_time - is the time when validator asks miners for prediction data
    #              and stores it in the database
    # scored_time - is the time when validator calculates rewards using the data
    #               from the database of previous prediction data
    scored_time = start_time

    # get latest prediction request from validator
    # for which we already have real prices data,
    # i.e. (start_time + time_length) < scored_time
    validator_request_id = miner_data_handler.get_latest_prediction_request(
        scored_time, simulation_input
    )
    if validator_request_id is None:
        time.sleep(3600)  # wait for an hour
        return

    # Adjust the scores based on responses from miners.
    # response[0] - miner_uuids[0]
    # this is the function we need to implement for our incentives mechanism,
    # it returns an array of floats that determines how good a particular miner was at price predictions:
    # example: [0.2, 0.7, 0.1] - you can see that the best miner was 2nd, and the worst 3rd
    rewards, rewards_detailed_info = get_rewards(
        miner_data_handler=miner_data_handler,
        price_data_provider=price_data_provider,
        simulation_input=simulation_input,
        miner_uids=miner_uids,
        validator_request_id=validator_request_id,
    )

    bt.logging.info(f"Scored responses: {rewards}")
    miner_data_handler.set_reward_details(
        reward_details=rewards_detailed_info, scored_time=scored_time
    )

    # apply custom moving average rewards
    miner_scores_df = miner_data_handler.get_miner_scores(scored_time, 2)
    moving_averages_data = compute_weighted_averages(
        input_df=miner_scores_df,
        half_life_days=1.0,
        alpha=2.0,
        validation_time_str=scored_time,
    )
    bt.logging.info(
        f"Scored responses moving averages: {moving_averages_data}"
    )
    if moving_averages_data is None:
        time.sleep(3600)
        return
    miner_data_handler.update_miner_rewards(moving_averages_data)

    # Update the scores based on the rewards.
    # You may want to define your own update_scores function for custom behavior.
    filtered_rewards, filtered_miner_uids = remove_zero_rewards(
        moving_averages_data
    )
    self.update_scores(np.array(filtered_rewards), filtered_miner_uids)
    time.sleep(3600)  # wait for an hour


def remove_zero_rewards(moving_averages_data):
    miners = []
    rewards = []
    for rewards_item in moving_averages_data:
        if rewards_item["reward_weight"] != 0:
            miners.append(rewards_item["miner_uid"])
            rewards.append(rewards_item["reward_weight"])
    return rewards, miners
