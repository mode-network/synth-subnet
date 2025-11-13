# The MIT License (MIT)
# Copyright © 2023 Yuma Rao
# Copyright © 2023 Mode Labs
import asyncio
from datetime import datetime, timedelta
import multiprocessing as mp

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


from dotenv import load_dotenv
import bittensor as bt

from synth.base.validator import BaseValidatorNeuron

from synth.simulation_input import SimulationInput
from synth.utils.helpers import (
    get_current_time,
    round_time_to_minutes,
    timeout_until,
)
from synth.utils.logging import setup_gcp_logging
from synth.utils.opening_hours import should_skip_xau
from synth.validator.forward import (
    calculate_moving_average_and_update_rewards,
    calculate_rewards_and_update_scores,
    get_available_miners_and_update_metagraph_history,
    query_available_miners_and_save_responses,
    send_weights_to_bittensor_and_update_weights_history,
)
from synth.validator.miner_data_handler import MinerDataHandler
from synth.validator.price_data_provider import PriceDataProvider


load_dotenv()


class Validator(BaseValidatorNeuron):
    """
    Your validator neuron class. You should use this class to define your validator's behavior. In particular, you should replace the forward function with your own logic.

    This class inherits from the BaseValidatorNeuron class, which in turn inherits from BaseNeuron. The BaseNeuron class takes care of routine tasks such as setting up wallet, subtensor, metagraph, logging directory, parsing config, etc. You can override any of the methods in BaseNeuron if you need to customize the behavior.

    This class provides reasonable default behavior for a validator such as keeping a moving average of the scores of the miners and using them to set weights at the end of each epoch. Additionally, the scores are reset for new hotkeys at the end of each epoch.
    """

    def __init__(self, config=None):
        super(Validator, self).__init__(config=config)

        setup_gcp_logging(self.config.gcp.log_id_prefix)

        bt.logging.info("load_state()")
        self.load_state()

        self.miner_data_handler = MinerDataHandler()
        self.price_data_provider = PriceDataProvider()

        self.simulation_input_list = [
            # input data: give me prediction of BTC price for the next 1 day for every 5 min of time
            SimulationInput(
                asset="BTC",
                time_increment=300,
                time_length=86400,
                num_simulations=1000,
            ),
            SimulationInput(
                asset="ETH",
                time_increment=300,
                time_length=86400,
                num_simulations=1000,
            ),
            SimulationInput(
                asset="XAU",
                time_increment=300,
                time_length=86400,
                num_simulations=1000,
            ),
            SimulationInput(
                asset="SOL",
                time_increment=300,
                time_length=86400,
                num_simulations=1000,
            ),
        ]
        self.timeout_extra_seconds = 60

        self.assert_assets_supported()

    def assert_assets_supported(self):
        # Assert assets are all implemented in the price data provider:
        for simulation in self.simulation_input_list:
            assert simulation.asset in PriceDataProvider.TOKEN_MAP

    async def forward_validator(self):
        """
        Validator forward pass. Consists of:
        - Generating the query
        - Querying the miners
        - Getting the responses
        - Rewarding the miners
        - Updating the scores
        """
        bt.logging.info("calling forward_validator()")
        return [
            asyncio.create_task(self.forward_prompt()),
        ]

    async def wait_till_next_simulation(
        self, request_time: datetime, simulation_input_list: list
    ):
        # wait until the next simulation
        next_iteration = request_time + timedelta(
            minutes=60 / len(simulation_input_list)
        )
        wait_time = timeout_until(next_iteration)
        bt.logging.info(
            f"Waiting for {wait_time/60} minutes until the next simulation",
            "forward_prompt",
        )
        await asyncio.sleep(wait_time)

    async def forward_prompt(self):
        for simulation_input in self.simulation_input_list:
            # ================= Step 1 ================= #
            # Getting available miners from metagraph and saving information about them
            # and their properties (rank, incentives, emission) at the current moment in the database
            # in the metagraph_history table and in the miners table
            # ========================================== #

            miner_uids = get_available_miners_and_update_metagraph_history(
                base_neuron=self,
                miner_data_handler=self.miner_data_handler,
            )

            if len(miner_uids) == 0:
                bt.logging.error(
                    "No miners available",
                    "forward_prompt",
                )
                await self.forward_score()
                await self.wait_till_next_simulation(
                    get_current_time(), self.simulation_input_list
                )
                continue

            request_time = get_current_time()
            start_time = round_time_to_minutes(
                request_time, 60, self.timeout_extra_seconds
            )

            if should_skip_xau(start_time) and simulation_input.asset == "XAU":
                bt.logging.info(
                    "Skipping XAU simulation as market is closed",
                    "forward_prompt",
                )
                await self.forward_score()
                await self.wait_till_next_simulation(
                    request_time, self.simulation_input_list
                )
                continue

            # ================= Step 2 ================= #
            # Query all the available miners and save all their responses
            # in the database in miner_predictions table
            # ========================================== #

            # add the start time to the simulation input
            simulation_input.start_time = start_time.isoformat()

            await query_available_miners_and_save_responses(
                base_neuron=self,
                miner_data_handler=self.miner_data_handler,
                miner_uids=miner_uids,
                simulation_input=simulation_input,
                request_time=request_time,
            )

            await self.forward_score()
            await self.wait_till_next_simulation(
                request_time, self.simulation_input_list
            )

    async def forward_score(self):
        current_time = get_current_time()

        # round current time to the closest minute and add extra minutes
        # to be sure we are after the start time of the prompt
        scored_time = round_time_to_minutes(
            current_time, 60, self.timeout_extra_seconds * 2
        )

        # ================= Step 3 ================= #
        # Calculate rewards based on historical predictions data
        # from the miner_predictions table:
        # we're going to get the predictions that are already in the past,
        # in this way we know the real prices, can compare them
        # with predictions and calculate the rewards,
        # we store the rewards in the miner_scores table
        # ========================================== #

        success = calculate_rewards_and_update_scores(
            miner_data_handler=self.miner_data_handler,
            price_data_provider=self.price_data_provider,
            scored_time=scored_time,
            cutoff_days=self.config.ewma.cutoff_days,
        )

        if not success:
            return

        # ================= Step 4 ================= #
        # Calculate moving average based on the past results
        # in the miner_scores table and save them
        # in the miner_rewards table in the end
        # ========================================== #

        moving_averages_data = calculate_moving_average_and_update_rewards(
            miner_data_handler=self.miner_data_handler,
            scored_time=scored_time,
            cutoff_days=self.config.ewma.cutoff_days,
            window_days=self.config.ewma.window_days,
            softmax_beta=self.config.softmax.beta,
        )

        if len(moving_averages_data) == 0:
            return

        # ================= Step 5 ================= #
        # Send rewards calculated in the previous step
        # into bittensor consensus calculation
        # ========================================== #

        moving_averages_data.append(
            {
                "miner_id": 0,
                "miner_uid": (
                    23 if self.config.subtensor.network == "test" else 248
                ),
                "smoothed_score": 0,
                "reward_weight": sum(
                    [r["reward_weight"] for r in moving_averages_data]
                ),
                "updated_at": scored_time.isoformat(),
            }
        )

        bt.logging.info(
            f"Moving averages data for owner: {moving_averages_data[-1]}"
        )

        send_weights_to_bittensor_and_update_weights_history(
            base_neuron=self,
            moving_averages_data=moving_averages_data,
            miner_data_handler=self.miner_data_handler,
            scored_time=scored_time,
        )

    async def forward_miner(self, _: bt.Synapse) -> bt.Synapse:
        pass


# The main function parses the configuration and runs the validator.
if __name__ == "__main__":
    mp.set_start_method("spawn", force=True)
    Validator().run()
