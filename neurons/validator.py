# The MIT License (MIT)
# Copyright © 2023 Yuma Rao
# Copyright © 2023 Mode Labs
import asyncio
from datetime import datetime
from typing import Optional
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
import httpx
import bittensor as bt

from synth.base.validator import BaseValidatorNeuron

from synth.simulation_input import SimulationInput
from synth.utils.async_scheduler import AsyncScheduler
from synth.utils.helpers import (
    get_current_time,
    round_time_to_minutes,
)
from synth.utils.logging import print_execution_time, setup_gcp_logging
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

    def __init__(self, config=None):
        super(Validator, self).__init__(config=config)

        setup_gcp_logging(self.config.gcp.log_id_prefix)

        bt.logging.info("load_state()", "__init__")
        self.load_state()

        self.miner_data_handler = MinerDataHandler()
        self.price_data_provider = PriceDataProvider()

        # Will be initialized in async context
        self._client: Optional[httpx.AsyncClient] = None
        self._schedulers: list[AsyncScheduler] = []

        self.miner_uids: list[int] = []

        PriceDataProvider.assert_assets_supported(HIGH_FREQUENCY.asset_list)
        PriceDataProvider.assert_assets_supported(LOW_FREQUENCY.asset_list)

    @property
    def client(self) -> httpx.AsyncClient:
        """Get the shared HTTP client"""
        if self._client is None:
            raise RuntimeError(
                "HTTP client not initialized. Call run() or initialize_async() first."
            )
        return self._client

    async def initialize_async(self):
        """Initialize async resources - call this before using async methods"""
        self._client = httpx.AsyncClient(
            http2=True,
            limits=httpx.Limits(
                max_connections=600,
                max_keepalive_connections=200,
            ),
            timeout=httpx.Timeout(60.0, connect=10.0),
        )

        # Create schedulers with async targets
        self._schedulers = [
            AsyncScheduler(
                LOW_FREQUENCY,
                self.cycle_low_frequency,
                self.miner_data_handler,
            ),
            AsyncScheduler(
                HIGH_FREQUENCY,
                self.cycle_high_frequency,
                self.miner_data_handler,
            ),
        ]

        bt.logging.info("Async resources initialized")

    async def forward_validator_async(self):
        """
        Async validator forward pass - all tasks run forever.
        """
        await self.initialize_async()

        try:
            self.miner_uids = (
                get_available_miners_and_update_metagraph_history(
                    base_neuron=self,
                    miner_data_handler=self.miner_data_handler,
                )
            )

            bt.logging.info(
                f"Starting {len(self._schedulers)} schedulers + scoring loop"
            )

            # All three run forever concurrently
            await asyncio.gather(
                self._schedulers[0].start(),  # Low frequency
                self._schedulers[1].start(),  # High frequency
                self._scoring_loop(),  # Scoring
            )
        except asyncio.CancelledError:
            bt.logging.exception("Validator cancelled")
            raise

    async def _scoring_loop(self):
        """Async scoring loop"""
        while True:
            try:
                await self.forward_score_async()
            except asyncio.CancelledError:
                bt.logging.info("Scoring loop cancelled")
                break
            except Exception:
                bt.logging.exception("Error in scoring loop")

            delay = 10
            bt.logging.info(
                f"Sleeping for {delay} seconds before next score calculation",
                "forward_validator_async",
            )
            await asyncio.sleep(delay)

    async def forward_score_async(self):
        """Async version of forward_score - implement your scoring logic here"""
        # Convert your existing forward_score to async
        # If it does blocking I/O, wrap with asyncio.to_thread()
        await asyncio.to_thread(self.forward_score)

    # Keep sync method for backward compatibility if needed
    def forward_validator(self):
        """Sync entry point - runs the async version"""
        asyncio.run(self.forward_validator_async())

    @print_execution_time
    async def cycle_low_frequency(self, asset: str):
        bt.logging.info(
            "starting the low frequency cycle", "cycle_low_frequency"
        )

        # update the miners, also for the high frequency prompt that will use the same list
        self.miner_uids = get_available_miners_and_update_metagraph_history(
            base_neuron=self,
            miner_data_handler=self.miner_data_handler,
        )
        await self.forward_prompt(asset, LOW_FREQUENCY)

    @print_execution_time
    async def cycle_high_frequency(self, asset: str):
        bt.logging.info(
            "starting the high frequency cycle", "cycle_high_frequency"
        )
        await self.forward_prompt(asset, HIGH_FREQUENCY)

    @print_execution_time
    async def forward_prompt(self, asset: str, prompt_config: PromptConfig):
        bt.logging.info(
            f"forward prompt for {asset} in {prompt_config.label} frequency",
            "forward_prompt",
        )
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

        simulation_input = SimulationInput(
            asset=asset,
            start_time=start_time.isoformat(),
            time_increment=prompt_config.time_increment,
            time_length=prompt_config.time_length,
            num_simulations=prompt_config.num_simulations,
        )

        await query_available_miners_and_save_responses(
            base_neuron=self,
            client=self.client,
            miner_data_handler=self.miner_data_handler,
            miner_uids=self.miner_uids,
            simulation_input=simulation_input,
            request_time=request_time,
        )

    @print_execution_time
    def forward_score(self):
        # ================= Step 3 ================= #
        # Calculate rewards based on historical predictions data
        # from the miner_predictions table:
        # we're going to get the predictions that are already in the past,
        # in this way we know the real prices, can compare them
        # with predictions and calculate the rewards,
        # we store the rewards in the miner_scores table
        # ========================================== #
        bt.logging.info(
            f"forward score {LOW_FREQUENCY.label} frequency", "forward_score"
        )
        current_time = get_current_time()
        scored_time: datetime = round_time_to_minutes(current_time)

        success_low = calculate_scores(
            self.miner_data_handler,
            self.price_data_provider,
            scored_time,
            LOW_FREQUENCY,
        )

        scored_time: datetime = round_time_to_minutes(current_time)
        current_time = get_current_time()
        bt.logging.info(
            f"forward score {HIGH_FREQUENCY.label} frequency", "forward_score"
        )
        success_high = calculate_scores(
            self.miner_data_handler,
            self.price_data_provider,
            scored_time,
            HIGH_FREQUENCY,
        )

        if not success_low and not success_high:
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
            f"Moving averages data for owner: {moving_averages_data[-1]}",
            "forward_score",
        )

        send_weights_to_bittensor_and_update_weights_history(
            base_neuron=self,
            moving_averages_data=moving_averages_data,
            miner_data_handler=self.miner_data_handler,
            scored_time=scored_time,
        )

        # self.cleanup_history()

    async def forward_miner(self, _: bt.Synapse) -> bt.Synapse:
        pass


# The main function parses the configuration and runs the validator.
if __name__ == "__main__":
    mp.set_start_method("spawn", force=True)
    Validator().run()
