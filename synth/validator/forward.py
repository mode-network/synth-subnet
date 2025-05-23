# The MIT License (MIT)
# Copyright © 2023 Yuma Rao
# Copyright © 2023 Mode Labs

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

from datetime import datetime, timedelta
import random
import asyncio


import bittensor as bt
import numpy as np


from synth.base.validator import BaseValidatorNeuron
from synth.protocol import Simulation
from synth.simulation_input import SimulationInput
from synth.utils.helpers import (
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


def send_weights_to_bittensor_and_update_weights_history(
    base_neuron: BaseValidatorNeuron,
    moving_averages_data: list[dict],
    miner_data_handler: MinerDataHandler,
    scored_time: datetime,
):
    miner_weights = [item["reward_weight"] for item in moving_averages_data]
    miner_uids = [item["miner_uid"] for item in moving_averages_data]

    base_neuron.update_scores(np.array(miner_weights), miner_uids)

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


def calculate_moving_average_and_update_rewards(
    miner_data_handler: MinerDataHandler,
    scored_time: datetime,
    cutoff_days: int,
    half_life_days: float,
    softmax_beta: float,
) -> list[dict]:
    # apply custom moving average rewards
    miner_scores_df = miner_data_handler.get_miner_scores(
        scored_time=scored_time,
        cutoff_days=cutoff_days,
    )

    df = prepare_df_for_moving_average(miner_scores_df)

    moving_averages_data = compute_weighted_averages(
        miner_data_handler=miner_data_handler,
        input_df=df,
        half_life_days=half_life_days,
        scored_time=scored_time,
        softmax_beta=softmax_beta,
    )

    if moving_averages_data is None:
        return []

    print_rewards_df(moving_averages_data)

    miner_data_handler.update_miner_rewards(moving_averages_data)

    return moving_averages_data


def calculate_rewards_and_update_scores(
    miner_data_handler: MinerDataHandler,
    price_data_provider: PriceDataProvider,
    scored_time: datetime,
    cutoff_days: int,
) -> bool:
    # get latest prediction request from validator
    validator_requests = miner_data_handler.get_latest_prediction_requests(
        scored_time, cutoff_days
    )

    if validator_requests is None or len(validator_requests) == 0:
        bt.logging.warning("No prediction requests found")
        return False

    bt.logging.debug(f"found {len(validator_requests)} prediction requests")

    fail_count = 0
    for validator_request in validator_requests:

        bt.logging.debug(f"validator_request_id: {validator_request.id}")

        prompt_scores, detailed_info = get_rewards(
            miner_data_handler=miner_data_handler,
            price_data_provider=price_data_provider,
            validator_request=validator_request,
        )

        print_scores_df(prompt_scores, detailed_info)

        if prompt_scores is None:
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


async def query_available_miners_and_save_responses(
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

    semaphore = asyncio.Semaphore(16)
    uid_to_query_task = {
        uid: asyncio.create_task(
            _query_miner(semaphore, base_neuron, synapse, uid, timeout)
        )
        for uid in miner_uids
    }
    synapses = await asyncio.gather(*uid_to_query_task.values())

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


async def _query_miner(
    semaphore: asyncio.Semaphore,
    base_neuron: BaseValidatorNeuron,
    synapse: bt.Synapse,
    uid: int,
    timeout: float,
) -> bt.Synapse:
    async with semaphore:
        result = await base_neuron.dendrite.forward(
            axons=base_neuron.metagraph.axons[uid],
            synapse=synapse,
            timeout=timeout,
            deserialize=False,
        )
    return result


def get_available_miners_and_update_metagraph_history(
    base_neuron: BaseValidatorNeuron,
    miner_data_handler: MinerDataHandler,
    start_time: datetime,
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
                "updated_at": start_time.isoformat(),
                "ip_address": base_neuron.metagraph.addresses[uid],
            }
            metagraph_info.append(metagraph_item)

    if len(miners) > 0:
        miner_data_handler.insert_new_miners(miners)

    if len(metagraph_info) > 0:
        miner_data_handler.update_metagraph_history(metagraph_info)

    random.shuffle(miner_uids)

    return miner_uids
