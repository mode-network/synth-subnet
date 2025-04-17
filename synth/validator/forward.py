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
from datetime import datetime, timedelta
import random

import bittensor as bt
import numpy as np
import wandb

from synth.base.validator import BaseValidatorNeuron
from synth.protocol import Simulation
from synth.simulation_input import SimulationInput
from synth.utils.helpers import (
    get_current_time,
    round_time_to_minutes,
    timeout_from_start_time,
    convert_list_elements_to_str,
)
from synth.utils.uids import check_uid_availability
from synth.validator.miner_data_handler import MinerDataHandler
from synth.validator.moving_average import (
    compute_weighted_averages,
    prepare_df_for_moving_average,
    print_rewards_df,
)
from synth.validator.price_data_provider import PriceDataProvider
from synth.validator.response_validation import validate_responses
from synth.validator.reward import get_rewards, print_scores_df


async def forward(
    base_neuron: BaseValidatorNeuron,
    miner_data_handler: MinerDataHandler,
    price_data_provider: PriceDataProvider,
):
    """
    The forward function is called by the validator every time step.

    It is responsible for querying the network and scoring the responses.

    Args:
        base_neuron (:obj:`bittensor.neuron.Neuron`): The neuron object which contains all the necessary state for the validator.
        miner_data_handler (:obj:`synth.validator.MinerDataHandler`): The MinerDataHandler object which contains all the necessary state for the validator.
        price_data_provider (:obj:`synth.validator.PriceDataProvider`): The PriceDataProvider returns real prices data for a specific token.
    """
    # getting current validation time
    request_time = get_current_time()

    # round validation time to the closest minute and add 1 extra minute
    start_time = round_time_to_minutes(request_time, 60, 120)

    # ================= Step 1 ================= #
    # Getting available miners from metagraph and saving information about them
    # and their properties (rank, incentives, emission) at the current moment in the database
    # in the metagraph_history table
    # ========================================== #

    miner_uids = _get_available_miners_and_update_metagraph_history(
        base_neuron=base_neuron,
        miner_data_handler=miner_data_handler,
        start_time=start_time,
    )

    if len(miner_uids) == 0:
        bt.logging.error("No miners available")
        _wait_till_next_iteration()
        return

    # ================= Step 2 ================= #
    # Query all the available miners and save all their responses
    # in the database in miner_predictions table
    # ========================================== #

    # input data: give me prediction of BTC price for the next 1 day for every 5 min of time
    simulation_input = SimulationInput(
        asset="BTC",
        start_time=start_time,
        time_increment=300,
        time_length=86400,
        num_simulations=100,
    )

    await _query_available_miners_and_save_responses(
        base_neuron=base_neuron,
        miner_data_handler=miner_data_handler,
        miner_uids=miner_uids,
        simulation_input=simulation_input,
        request_time=request_time,
    )

    # ================= Step 3 ================= #
    # Calculate rewards based on historical predictions data
    # from the miner_predictions table:
    # we're going to get the prediction that is already in the past,
    # in this way we know the real prices, can compare them
    # with predictions and calculate the rewards,
    # we store the rewards in the miner_scores table
    # ========================================== #

    # scored_time is the same as start_time for a single validator step
    # but the meaning is different
    # start_time - is the time when validator asks miners for prediction data
    #              and stores it in the database
    # scored_time - is the time when validator calculates rewards using the data
    #               from the database of previous prediction data
    scored_time = start_time

    success = _calculate_rewards_and_update_scores(
        miner_data_handler=miner_data_handler,
        price_data_provider=price_data_provider,
        scored_time=scored_time,
        simulation_input=simulation_input,
        cutoff_days=base_neuron.config.ewma.cutoff_days,
    )

    if not success:
        _wait_till_next_iteration()
        return

    # ================= Step 4 ================= #
    # Calculate moving average based on the past results
    # in the miner_scores table and save them
    # in the miner_rewards table in the end
    # ========================================== #

    moving_averages_data = _calculate_moving_average_and_update_rewards(
        miner_data_handler=miner_data_handler,
        scored_time=scored_time,
        cutoff_days=base_neuron.config.ewma.cutoff_days,
        half_life_days=base_neuron.config.ewma.half_life_days,
        softmax_beta=base_neuron.config.softmax.beta,
    )

    if len(moving_averages_data) == 0:
        _wait_till_next_iteration()
        return

    # ================= Step 5 ================= #
    # Send rewards calculated in the previous step
    # into bittensor consensus calculation
    # ========================================== #

    _send_weights_to_bittensor_and_update_weights_history(
        base_neuron=base_neuron,
        moving_averages_data=moving_averages_data,
        miner_data_handler=miner_data_handler,
        scored_time=scored_time,
    )

    _wait_till_next_iteration()


def _send_weights_to_bittensor_and_update_weights_history(
    base_neuron: BaseValidatorNeuron,
    moving_averages_data: list[dict],
    miner_data_handler: MinerDataHandler,
    scored_time: str,
):
    miner_weights = [item["reward_weight"] for item in moving_averages_data]
    miner_uids = [item["miner_uid"] for item in moving_averages_data]

    base_neuron.update_scores(np.array(miner_weights), miner_uids)

    wandb_on = base_neuron.config.wandb.enabled
    _log_to_wandb(wandb_on, miner_uids, miner_weights)

    base_neuron.resync_metagraph()
    result, msg, uint_uids, uint_weights = base_neuron.set_weights()

    if result:
        bt.logging.info("set_weights on chain successfully!")
        msg = "SUCCESS"
    else:
        rate_limit_message = "Perhaps it is too soon to commit weights"
        if rate_limit_message in msg:
            bt.logging.warning(msg, "set_weights failed")
        else:
            bt.logging.error(msg, "set_weights failed")

    miner_data_handler.update_weights_history(
        miner_uids=miner_uids,
        miner_weights=miner_weights,
        norm_miner_uids=convert_list_elements_to_str(uint_uids),
        norm_miner_weights=convert_list_elements_to_str(uint_weights),
        update_result=msg,
        scored_time=scored_time,
    )


def _wait_till_next_iteration():
    time.sleep(3600)  # wait for an hour


def _calculate_moving_average_and_update_rewards(
    miner_data_handler: MinerDataHandler,
    scored_time: str,
    cutoff_days: int,
    half_life_days: float,
    softmax_beta: float,
) -> list[dict]:
    # apply custom moving average rewards
    miner_scores_df = miner_data_handler.get_miner_scores(
        scored_time_str=scored_time,
        cutoff_days=cutoff_days,
    )

    df = prepare_df_for_moving_average(miner_scores_df)

    moving_averages_data = compute_weighted_averages(
        miner_data_handler=miner_data_handler,
        input_df=df,
        half_life_days=half_life_days,
        scored_time_str=scored_time,
        softmax_beta=softmax_beta,
    )

    if moving_averages_data is None:
        return []

    print_rewards_df(moving_averages_data)

    miner_data_handler.update_miner_rewards(moving_averages_data)

    return moving_averages_data


def _calculate_rewards_and_update_scores(
    miner_data_handler: MinerDataHandler,
    price_data_provider: PriceDataProvider,
    scored_time: str,
    simulation_input: SimulationInput,
    cutoff_days: int,
) -> bool:
    # get latest prediction request from validator
    validator_requests = miner_data_handler.get_latest_prediction_requests(
        scored_time, simulation_input, cutoff_days
    )

    if validator_requests is None or len(validator_requests) == 0:
        bt.logging.warning("No prediction requests found")
        return False

    bt.logging.trace(f"found {len(validator_requests)} prediction requests")

    fail_count = 0
    for validator_request in validator_requests:

        bt.logging.trace(f"validator_request_id: {validator_request.id}")

        prompt_scores_v2, detailed_info = get_rewards(
            miner_data_handler=miner_data_handler,
            price_data_provider=price_data_provider,
            validator_request=validator_request,
        )

        print_scores_df(prompt_scores_v2, detailed_info)

        if prompt_scores_v2 is None:
            bt.logging.warning("No rewards calculated")
            fail_count += 1
            continue

        miner_score_time = validator_request.start_time + timedelta(
            seconds=validator_request.time_length
        )

        miner_data_handler.set_miner_scores(
            reward_details=detailed_info, scored_time=miner_score_time
        )

    # Success if at least one request succeed
    return fail_count != len(validator_requests)


async def _query_available_miners_and_save_responses(
    base_neuron: BaseValidatorNeuron,
    miner_data_handler: MinerDataHandler,
    miner_uids: list,
    simulation_input: SimulationInput,
    request_time: datetime,
):
    timeout = timeout_from_start_time(
        base_neuron.config.neuron.timeout, simulation_input.start_time
    )

    # synapse - is a message that validator sends to miner to get results, i.e. simulation_input in our case
    # Simulation - is our protocol, i.e. input and output message of a miner (application that returns prediction of
    # prices for a chosen asset)
    synapse = Simulation(simulation_input=simulation_input)
    # The dendrite client queries the network:
    # it is the actual call to all the miners from validator
    # returns an array of synapses (predictions) for each of the miners
    # ======================================================
    # miner has a unique uuid in the subnetwork
    # ======================================================
    # axon is a server application that accepts requests on the miner side
    # ======================================================
    synapses = await base_neuron.dendrite(
        axons=[base_neuron.metagraph.axons[uid] for uid in miner_uids],
        synapse=synapse,
        deserialize=False,
        timeout=timeout,
    )

    miner_predictions = {}
    for i, synapse in enumerate(synapses):
        response = synapse.deserialize()
        process_time = synapse.dendrite.process_time
        format_validation = validate_responses(
            response, simulation_input, request_time, process_time
        )
        miner_id = miner_uids[i]
        miner_predictions[miner_id] = (
            response,
            format_validation,
            process_time,
        )

    if len(miner_predictions) > 0:
        miner_data_handler.save_responses(
            miner_predictions, simulation_input, request_time
        )
    else:
        bt.logging.info("skip saving because no prediction")


def _get_available_miners_and_update_metagraph_history(
    base_neuron: BaseValidatorNeuron,
    miner_data_handler: MinerDataHandler,
    start_time: str,
):
    miner_uids = []
    miners = []
    metagraph_info = []
    for uid in range(len(base_neuron.metagraph.S)):
        uid_is_available = check_uid_availability(
            base_neuron.metagraph,
            uid,
            base_neuron.config.neuron.vpermit_tao_limit,
        )

        # adding the uid even if not available, to generate a score
        miner_uids.append(uid)
        miners.append(
            {
                "neuron_uid": uid,
                "coldkey": base_neuron.metagraph.coldkeys[uid],
                "hotkey": base_neuron.metagraph.hotkeys[uid],
            }
        )

        if uid_is_available:
            metagraph_item = {
                "neuron_uid": uid,
                "incentive": float(base_neuron.metagraph.I[uid]),
                "rank": float(base_neuron.metagraph.R[uid]),
                "stake": float(base_neuron.metagraph.S[uid]),
                "trust": float(base_neuron.metagraph.T[uid]),
                "emission": float(base_neuron.metagraph.E[uid]),
                "pruning_score": float(
                    base_neuron.metagraph.pruning_score[uid]
                ),
                "coldkey": base_neuron.metagraph.coldkeys[uid],
                "hotkey": base_neuron.metagraph.hotkeys[uid],
                "updated_at": start_time,
            }
            metagraph_info.append(metagraph_item)

    if len(miners) > 0:
        miner_data_handler.insert_new_miners(miners)

    if len(metagraph_info) > 0:
        miner_data_handler.update_metagraph_history(metagraph_info)

    random.shuffle(miner_uids)

    return miner_uids


def _log_to_wandb(wandb_on, miner_uids, rewards):
    if wandb_on:
        # Log results to wandb
        wandb_val_log = {
            "miners_info": {
                miner_uid: {
                    "miner_reward": reward,
                }
                for miner_uid, reward in zip(miner_uids, rewards)
            }
        }
        wandb.log(wandb_val_log)
