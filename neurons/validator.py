# The MIT License (MIT)
# Copyright © 2023 Yuma Rao
# Copyright © 2023 Mode Labs
import os
import asyncio
from datetime import timedelta

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


import bittensor as bt
import wandb

from synth.base.validator import BaseValidatorNeuron

from synth.simulation_input import SimulationInput
from synth.utils.helpers import (
    get_current_time,
    round_time_to_minutes,
    timeout_until,
)
from synth.utils.logging import setup_wandb_alert
from synth.validator.forward import (
    calculate_moving_average_and_update_rewards,
    calculate_rewards_and_update_scores,
    get_available_miners_and_update_metagraph_history,
    query_available_miners_and_save_responses,
    send_weights_to_bittensor_and_update_weights_history,
)
from synth.validator.miner_data_handler import MinerDataHandler
from synth.validator.price_data_provider import PriceDataProvider

from synth import __version__


class Validator(BaseValidatorNeuron):
    """
    Your validator neuron class. You should use this class to define your validator's behavior. In particular, you should replace the forward function with your own logic.

    This class inherits from the BaseValidatorNeuron class, which in turn inherits from BaseNeuron. The BaseNeuron class takes care of routine tasks such as setting up wallet, subtensor, metagraph, logging directory, parsing config, etc. You can override any of the methods in BaseNeuron if you need to customize the behavior.

    This class provides reasonable default behavior for a validator such as keeping a moving average of the scores of the miners and using them to set weights at the end of each epoch. Additionally, the scores are reset for new hotkeys at the end of each epoch.
    """

    def __init__(self, config=None):
        super(Validator, self).__init__(config=config)

        bt.logging.info("load_state()")
        self.load_state()

        self.miner_data_handler = MinerDataHandler()
        self.price_data_provider = PriceDataProvider()
        self.wandb_handler = None

        self.simulation_input_list = [
            # input data: give me prediction of BTC price for the next 1 day for every 5 min of time
            SimulationInput(
                asset="BTC",
                time_increment=300,
                time_length=86400,
                num_simulations=100,
            ),
            # SimulationInput(
            #     asset="ETH",
            #     time_increment=300,
            #     time_length=86400,
            #     num_simulations=100,
            # ),
        ]
        self.len_simulations = len(self.simulation_input_list)
        self.timeout_extra_seconds = 120

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
        wandb_api_key = os.getenv("WANDB_API_KEY")
        if wandb_api_key is not None and self.config.wandb.enabled:
            bt.logging.info("WANDB_API_KEY is set")
            run = wandb.init(
                project=f"{self.config.wandb.project_name}",
                mode=(
                    "disabled"
                    if not getattr(self.config.wandb, "enabled", False)
                    else "online"
                ),
                entity=f"{self.config.wandb.entity}",
                config={
                    "hotkey": self.wallet.hotkey.ss58_address,
                },
                name=f"validator-{self.uid}-{__version__}",
                resume="auto",
                dir=self.config.neuron.full_path,
                reinit=True,
            )
            if self.config.subtensor.network != "test":
                if self.wandb_handler is not None:
                    bt.logging._logger.removeHandler(self.wandb_handler)
                self.wandb_handler = setup_wandb_alert(run)
                bt.logging._logger.addHandler(self.wandb_handler)
        else:
            bt.logging.warning(
                "WANDB_API_KEY not found in environment variables."
            )

        bt.logging.info("calling forward_validator()")
        return [
            asyncio.create_task(self.forward_prompt()),
            asyncio.create_task(self.forward_score()),
        ]

    async def forward_prompt(self):
        # getting current validation time
        request_time = get_current_time()
        next_iteration = request_time + timedelta(hours=1)

        async def wait_till_next_simulation():
            # wait until the next simulation
            wait_time = timeout_until(next_iteration) / self.len_simulations
            bt.logging.info(
                f"Waiting for {wait_time} seconds until the next simulation",
                "forward_prompt",
            )
            await asyncio.sleep(wait_time)

        for simulation_index in range(self.len_simulations):
            # round validation time to the closest minute and add extra minutes
            start_time = round_time_to_minutes(
                request_time, 60, self.timeout_extra_seconds
            )

            # ================= Step 1 ================= #
            # Getting available miners from metagraph and saving information about them
            # and their properties (rank, incentives, emission) at the current moment in the database
            # in the metagraph_history table and in the miners table
            # ========================================== #

            miner_uids = get_available_miners_and_update_metagraph_history(
                base_neuron=self,
                miner_data_handler=self.miner_data_handler,
                start_time=start_time,
            )

            if len(miner_uids) == 0:
                bt.logging.error("No miners available")
                await wait_till_next_simulation()
                return

            # ================= Step 2 ================= #
            # Query all the available miners and save all their responses
            # in the database in miner_predictions table
            # ========================================== #

            # input data: from the list declared above, at the index of the loop
            simulation_input = self.simulation_input_list[simulation_index]
            # add the start time
            simulation_input.start_time = start_time.isoformat()

            await query_available_miners_and_save_responses(
                base_neuron=self,
                miner_data_handler=self.miner_data_handler,
                miner_uids=miner_uids,
                simulation_input=simulation_input,
                request_time=request_time,
            )

            await wait_till_next_simulation()
            request_time = get_current_time()

    async def forward_score(self):
        # getting current time
        current_time = get_current_time()

        next_iteration = current_time + timedelta(hours=1)

        async def wait_till_next_iteration():
            # wait until the next iteration
            wait_time = timeout_until(next_iteration)
            bt.logging.info(
                f"Waiting for {wait_time} seconds until the next iteration",
                "forward_score",
            )
            await asyncio.sleep(wait_time)

        # round current time to the closest minute and add extra minutes
        # to be sure we are after the start time of the prompt
        scored_time = round_time_to_minutes(
            current_time, 60, self.timeout_extra_seconds * 2
        )

        # wait until the score_time
        wait_time = timeout_until(scored_time)
        bt.logging.info(
            f"Waiting for {wait_time} seconds to start validating",
            "forward_score",
        )
        await asyncio.sleep(wait_time)

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
            await wait_till_next_iteration()
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
            half_life_days=self.config.ewma.half_life_days,
            softmax_beta=self.config.softmax.beta,
        )

        if len(moving_averages_data) == 0:
            await wait_till_next_iteration()
            return

        # ================= Step 5 ================= #
        # Send rewards calculated in the previous step
        # into bittensor consensus calculation
        # ========================================== #

        send_weights_to_bittensor_and_update_weights_history(
            base_neuron=self,
            moving_averages_data=moving_averages_data,
            miner_data_handler=self.miner_data_handler,
            scored_time=scored_time,
        )

        await wait_till_next_iteration()

    async def forward_miner(self, _: bt.Synapse) -> bt.Synapse:
        pass


# The main function parses the configuration and runs the validator.
if __name__ == "__main__":
    Validator().run()
