import asyncio
from datetime import datetime, timedelta
from typing import Callable, Awaitable, Optional
import bittensor as bt

from synth.validator.miner_data_handler import MinerDataHandler
from synth.validator.prompt_config import PromptConfig
from synth.utils.helpers import (
    get_current_time,
    round_time_to_minutes,
    new_equities_launch,
)


class AsyncScheduler:
    """Pure async scheduler - replaces ThreadScheduler"""

    def __init__(
        self,
        prompt_config: PromptConfig,
        target: Callable[[str], Awaitable],
        miner_data_handler: MinerDataHandler,
    ):
        self.prompt_config = prompt_config
        self.target = target
        self.miner_data_handler = miner_data_handler
        self._running = False
        self._task: Optional[asyncio.Task] = None

    async def start(self):
        """Start the scheduling loop"""
        latest_asset = None

        bt.logging.info(
            f"AsyncScheduler started for {self.prompt_config.label}"
        )

        while True:
            try:
                cycle_start_time = get_current_time()

                # Get asset list
                asset_list = self._get_asset_list()

                # Select next asset
                if latest_asset is None:
                    latest_asset = self.miner_data_handler.get_latest_asset(
                        self.prompt_config.time_length
                    )

                asset = self.select_asset(latest_asset, asset_list)
                latest_asset = asset  # For next iteration

                # Calculate delay until next cycle
                delay = self.select_delay(
                    asset_list, cycle_start_time, self.prompt_config
                )

                bt.logging.info(
                    f"Scheduling next {self.prompt_config.label} cycle "
                    f"for asset {asset} in {delay} seconds"
                )

                # Wait until next cycle
                if delay > 0:
                    await asyncio.sleep(delay)

                # Run the target with timeout
                target_timeout = 60 * 10  # 10 minutes
                try:
                    bt.logging.info(
                        f"Starting {self.prompt_config.label} cycle for {asset}"
                    )
                    await asyncio.wait_for(
                        self.target(asset),
                        timeout=target_timeout,
                    )
                except asyncio.TimeoutError:
                    bt.logging.error(
                        f"Target timed out after {target_timeout}s for asset {asset} "
                        f"{self.prompt_config.label}"
                    )
                except asyncio.CancelledError:
                    bt.logging.error(f"Cycle cancelled for {asset}")

            except asyncio.CancelledError:
                bt.logging.error(
                    f"Scheduler {self.prompt_config.label} cancelled"
                )
            except Exception:
                bt.logging.exception(
                    f"Error in {self.prompt_config.label} cycle for asset {asset}"
                )
                # Brief pause before retry to avoid tight error loops
                await asyncio.sleep(5)

        bt.logging.info(
            f"AsyncScheduler stopped for {self.prompt_config.label}"
        )

    def _get_asset_list(self) -> list[str]:
        asset_list = self.prompt_config.asset_list[:6]
        if get_current_time() <= new_equities_launch:
            asset_list = asset_list[:4]
        return asset_list

    @staticmethod
    def select_delay(
        asset_list: list[str],
        cycle_start_time: datetime,
        prompt_config: PromptConfig,
    ) -> int:
        delay = prompt_config.initial_delay
        next_cycle = cycle_start_time + timedelta(
            minutes=prompt_config.total_cycle_minutes / len(asset_list)
        )
        next_cycle = round_time_to_minutes(next_cycle)
        next_cycle = next_cycle - timedelta(minutes=1)
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
