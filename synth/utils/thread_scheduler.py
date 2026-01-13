from datetime import datetime, timedelta
from threading import Timer
import asyncio


import bittensor as bt


from synth.validator.miner_data_handler import MinerDataHandler
from synth.validator.prompt_config import PromptConfig
from synth.utils.helpers import (
    get_current_time,
    round_time_to_minutes,
)


class ThreadScheduler:
    def __init__(
        self,
        prompt_config: PromptConfig,
        target: callable,
        miner_data_handler: MinerDataHandler,
    ):
        self.prompt_config = prompt_config
        self.target = target
        self.miner_data_handler = miner_data_handler

    def enter(self, *args):
        cycle_start_time = get_current_time()
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.target(*args))
        loop.close()
        self.schedule_cycle(cycle_start_time)

    def schedule_cycle(
        self, cycle_start_time: datetime, immediately: bool = False
    ):
        prompt_config = self.prompt_config

        delay = self.select_delay(
            prompt_config.asset_list,
            cycle_start_time,
            prompt_config,
            immediately,
        )
        latest_asset = self.miner_data_handler.get_latest_asset(
            prompt_config.time_length
        )
        asset = self.select_asset(latest_asset, prompt_config.asset_list)

        bt.logging.info(
            f"Scheduling next {prompt_config.label} frequency cycle for asset {asset} in {delay} seconds"
        )

        self.thread = Timer(delay, self.enter, (asset,))
        self.thread.start()

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
            next_cycle_diff = next_cycle - get_current_time()
            delay = int(next_cycle_diff.total_seconds())
            if delay < 0:
                delay = 0

        return delay

    @staticmethod
    def select_asset(latest_asset: str | None, asset_list: list[str]) -> str:
        asset = asset_list[0]

        if latest_asset is not None and latest_asset in asset_list:
            latest_index = asset_list.index(latest_asset)
            asset = asset_list[(latest_index + 1) % len(asset_list)]

        return asset
