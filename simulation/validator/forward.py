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

from simulation.base.validator import BaseValidatorNeuron
from simulation.protocol import Simulation
from simulation.simulation_input import SimulationInput
from simulation.utils.helpers import get_current_time, round_time_to_minutes
from simulation.utils.uids import check_uid_availability
from simulation.validator.miner_data_handler import MinerDataHandler
from simulation.validator.price_data_provider import PriceDataProvider
from simulation.validator.reward import get_rewards


async def forward(
        self: BaseValidatorNeuron,
        miner_data_handler: MinerDataHandler,
        price_data_provider: PriceDataProvider):
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
    for uid in range(len(self.metagraph.S)):
        uid_is_available = check_uid_availability(
            self.metagraph, uid, self.config.neuron.vpermit_tao_limit
        )
        if uid_is_available:
            miner_uids.append(uid)

    # input data
    # give me prediction of BTC price for the next 1 day for every 5 min of time
    simulation_input = SimulationInput(
        asset="BTC",
        start_time=start_time,
        time_increment=300,
        time_length=86400,
        num_simulations=1
    )

    # synapse - is a message that validator sends to miner to get results, i.e. simulation_input in our case
    # Simulation - is our protocol, i.e. input and output message of a miner (application that returns prediction of
    # prices for a chosen asset)
    synapse = Simulation(
        simulation_input=simulation_input
    )

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

    for i, response in enumerate(responses):
        if response is None or len(response) == 0:
            continue
        miner_id = miner_uids[i]
        miner_data_handler.set_values(miner_id, start_time, response)

    # Adjust the scores based on responses from miners.
    # response[0] - miner_uuids[0]
    # this is the function we need to implement for our incentives mechanism,
    # it returns an array of floats that determines how good a particular miner was at price predictions:
    # example: [0.2, 0.8, 0.1] - you can see that the best miner was 2nd, and the worst 3rd
    rewards, rewards_detailed_info = get_rewards(
        miner_data_handler=miner_data_handler,
        simulation_input=simulation_input,
        miner_uids=miner_uids,
        validation_time=start_time,
        price_data_provider=price_data_provider,
    )

    bt.logging.info(f"Scored responses: {rewards}")
    miner_data_handler.set_reward_details(rewards_detailed_info, start_time)

    # Update the scores based on the rewards.
    # You may want to define your own update_scores function for custom behavior.
    filtered_rewards, filtered_miner_uids = remove_zero_rewards(rewards, miner_uids)
    self.update_scores(filtered_rewards, filtered_miner_uids)
    time.sleep(3600)  # wait for an hour


def remove_zero_rewards(rewards, miner_uids):
    mask = rewards != 0
    filtered_rewards = rewards[mask]
    filtered_miners_uid = [miner_uids[i] for i in range(len(mask)) if mask[i]]

    return filtered_rewards, filtered_miners_uid
