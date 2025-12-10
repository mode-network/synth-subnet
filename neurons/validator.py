# The MIT License (MIT)
# Copyright © 2023 Yuma Rao
# Copyright © 2023 Mode Labs
from datetime import datetime, timedelta
import multiprocessing as mp
import sched
import time

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
)
from synth.utils.logging import setup_gcp_logging
from synth.utils.opening_hours import should_skip_xau
from synth.validator.forward import (
    calculate_moving_average_and_update_rewards,
    calculate_scores,
    get_available_miners_and_update_metagraph_history,
    query_available_miners_and_save_responses,
    send_weights_to_bittensor_and_update_weights_history,
)
from synth.validator.miner_data_handler import MinerDataHandler
from synth.validator.price_data_provider import PriceDataProvider
from synth.validator.prompt_config import (
    PromptConfig,
    LOW_FREQUENCY,
    HIGH_FREQUENCY,
)


load_dotenv()


class Validator(BaseValidatorNeuron):
    """
    Your validator neuron class. You should use this class to define your validator's behavior. In particular, you should replace the forward function with your own logic.

    This class inherits from the BaseValidatorNeuron class, which in turn inherits from BaseNeuron. The BaseNeuron class takes care of routine tasks such as setting up wallet, subtensor, metagraph, logging directory, parsing config, etc. You can override any of the methods in BaseNeuron if you need to customize the behavior.

    This class provides reasonable default behavior for a validator such as keeping a moving average of the scores of the miners and using them to set weights at the end of each epoch. Additionally, the scores are reset for new hotkeys at the end of each epoch.
    """

    asset_list = ["BTC", "ETH", "XAU", "SOL"]

    def __init__(self, config=None):
        super(Validator, self).__init__(config=config)

        setup_gcp_logging(self.config.gcp.log_id_prefix)

        bt.logging.info("load_state()")
        self.load_state()

        self.miner_data_handler = MinerDataHandler()
        self.price_data_provider = PriceDataProvider()

        self.scheduler = sched.scheduler(time.time, time.sleep)
        self.miner_uids: list[int] = []
        HIGH_FREQUENCY.softmax_beta = self.config.softmax.beta

        self.assert_assets_supported()

    def assert_assets_supported(self):
        # Assert assets are all implemented in the price data provider:
        for asset in self.asset_list:
            assert asset in PriceDataProvider.TOKEN_MAP

    def forward_validator(self):
        """
        Validator forward pass. Consists of:
        - Generating the query
        - Querying the miners
        - Getting the responses
        - Rewarding the miners
        - Updating the scores
        """
        self.miner_uids = get_available_miners_and_update_metagraph_history(
            base_neuron=self,
            miner_data_handler=self.miner_data_handler,
        )
        self.schedule_cycle(get_current_time(), HIGH_FREQUENCY, True)
        self.schedule_cycle(get_current_time(), LOW_FREQUENCY, True)
        self.scheduler.run()

    def schedule_cycle(
        self,
        cycle_start_time: datetime,
        prompt_config: PromptConfig,
        immediately: bool = False,
    ):
        delay = self.select_delay(
            self.asset_list, cycle_start_time, prompt_config, immediately
        )
        latest_asset = self.miner_data_handler.get_latest_asset(
            prompt_config.time_length
        )
        asset = self.select_asset(latest_asset, self.asset_list, delay)

        bt.logging.info(
            f"Scheduling next {prompt_config.label} frequency cycle for asset {asset} in {delay} seconds"
        )

        method = (
            self.cycle_low_frequency
            if prompt_config.label == LOW_FREQUENCY.label
            else self.cycle_high_frequency
        )
        self.scheduler.enter(
            delay=delay,
            priority=1,
            action=method,
            argument=(asset,),
        )

    @staticmethod
    def select_delay(
        asset_list: list[str],
        cycle_start_time: datetime,
        prompt_config: PromptConfig,
        immediately: bool,
    ) -> int:
        delay = prompt_config.initial_delay
        if not immediately:
            next_cycle = cycle_start_time + timedelta(
                minutes=prompt_config.total_cycle_minutes / len(asset_list)
            )
            next_cycle = round_time_to_minutes(next_cycle)
            next_cycle_diff = next_cycle - get_current_time()
            delay = int(next_cycle_diff.total_seconds())
            if delay < 0:
                delay = 0

        return delay

    @staticmethod
    def select_asset(
        latest_asset: str | None, asset_list: list[str], delay: int
    ) -> str:
        asset = asset_list[0]

        if latest_asset is not None and latest_asset in asset_list:
            latest_index = asset_list.index(latest_asset)
            asset = asset_list[(latest_index + 1) % len(asset_list)]

        future_start_time = get_current_time() + timedelta(seconds=delay)
        future_start_time = round_time_to_minutes(future_start_time)
        if should_skip_xau(future_start_time) and asset == "XAU":
            asset = asset_list[(asset_list.index("XAU") + 1) % len(asset_list)]

        return asset

    def cycle_low_frequency(self, asset: str):
        bt.logging.info(f"starting the {LOW_FREQUENCY.label} frequency cycle")
        cycle_start_time = get_current_time()

        # update the miners, also for the high frequency prompt that will use the same list
        self.miner_uids = get_available_miners_and_update_metagraph_history(
            base_neuron=self,
            miner_data_handler=self.miner_data_handler,
        )
        self.forward_prompt(asset, LOW_FREQUENCY)
        self.forward_score_low_frequency()
        # self.cleanup_history()
        self.sync()
        self.schedule_cycle(cycle_start_time, LOW_FREQUENCY)

    def cycle_high_frequency(self, asset: str):
        cycle_start_time = get_current_time()

        self.forward_prompt(asset, HIGH_FREQUENCY)

        current_time = get_current_time()
        scored_time: datetime = round_time_to_minutes(current_time)
        bt.logging.info(f"forward score {HIGH_FREQUENCY.label} frequency")
        calculate_scores(
            self.miner_data_handler,
            self.price_data_provider,
            scored_time,
            HIGH_FREQUENCY,
        )
        self.schedule_cycle(cycle_start_time, HIGH_FREQUENCY)

    def forward_prompt(self, asset: str, prompt_config: PromptConfig):
        bt.logging.info(f"forward prompt for {prompt_config.label} frequency")
        if len(self.miner_uids) == 0:
            bt.logging.error(
                "No miners available",
                "forward_prompt",
            )
            return

        request_time = get_current_time()
        start_time: datetime = round_time_to_minutes(
            request_time, prompt_config.timeout_extra_seconds
        )

        if should_skip_xau(start_time) and asset == "XAU":
            bt.logging.info(
                "Skipping XAU simulation as market is closed",
                "forward_prompt",
            )
            return

        simulation_input = SimulationInput(
            asset=asset,
            start_time=start_time.isoformat(),
            time_increment=prompt_config.time_increment,
            time_length=prompt_config.time_length,
            num_simulations=prompt_config.num_simulations,
        )

        query_available_miners_and_save_responses(
            base_neuron=self,
            miner_data_handler=self.miner_data_handler,
            miner_uids=self.miner_uids,
            simulation_input=simulation_input,
            request_time=request_time,
        )

    def forward_score_low_frequency(self):
        bt.logging.info(f"forward score {LOW_FREQUENCY.label} frequency")
        current_time = get_current_time()
        scored_time: datetime = round_time_to_minutes(current_time)

        # ================= Step 3 ================= #
        # Calculate rewards based on historical predictions data
        # from the miner_predictions table:
        # we're going to get the predictions that are already in the past,
        # in this way we know the real prices, can compare them
        # with predictions and calculate the rewards,
        # we store the rewards in the miner_scores table
        # ========================================== #

        success = calculate_scores(
            self.miner_data_handler,
            self.price_data_provider,
            scored_time,
            LOW_FREQUENCY,
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
