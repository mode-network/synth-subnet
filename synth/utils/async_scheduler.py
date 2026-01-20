import asyncio
import time
from datetime import datetime, timedelta
from typing import Callable, Awaitable
import bittensor as bt

from synth.validator.miner_data_handler import MinerDataHandler
from synth.validator.prompt_config import PromptConfig
from synth.utils.helpers import (
    get_current_time,
    round_time_to_minutes,
    new_equities_launch,
)


class AsyncScheduler:
    """
    Pure async scheduler that fires cycles without waiting for completion.
    Multiple cycles can run concurrently.
    """

    def __init__(
        self,
        prompt_config: PromptConfig,
        target: Callable[[str], Awaitable],
        miner_data_handler: MinerDataHandler,
    ):
        self.prompt_config = prompt_config
        self.target = target
        self.miner_data_handler = miner_data_handler
        self.first_run = True

    async def start(self):
        """Start the scheduling loop - fires cycles without waiting"""
        latest_asset = None

        bt.logging.info(
            f"AsyncScheduler started for {self.prompt_config.label}"
        )

        while True:
            try:
                cycle_start_time = get_current_time()
                asset_list = self._get_asset_list()

                if latest_asset is None:
                    latest_asset = self.miner_data_handler.get_latest_asset(
                        self.prompt_config.time_length
                    )

                asset = self.select_asset(latest_asset, asset_list)
                latest_asset = asset

                delay = self.select_delay(
                    asset_list,
                    cycle_start_time,
                    self.prompt_config,
                    self.first_run,
                )

                bt.logging.info(
                    f"Scheduling {self.prompt_config.label} cycle for {asset} "
                    f"in {delay}s"
                )

                if delay > 0:
                    await asyncio.sleep(delay)

                # FIRE AND FORGET - don't await!
                asyncio.create_task(
                    self._run_cycle(asset),
                    name=f"{self.prompt_config.label}_{asset}_{int(time.time())}",
                )

                self.first_run = False

                # Immediately continue loop to schedule next

            except asyncio.CancelledError:
                bt.logging.error(
                    f"Scheduler {self.prompt_config.label} cancelled"
                )
            except Exception:
                bt.logging.exception(
                    f"Error in scheduler {self.prompt_config.label}"
                )
                await asyncio.sleep(5)

    async def _run_cycle(self, asset: str):
        """Run a single cycle with timeout and error handling"""
        target_timeout = 60 * 10 * 3  # seconds

        try:
            bt.logging.info(
                f"Starting {self.prompt_config.label} cycle for {asset}"
            )

            await asyncio.wait_for(
                self.target(asset),
                timeout=target_timeout,
            )

            bt.logging.info(
                f"Completed {self.prompt_config.label} cycle for {asset}"
            )

        except asyncio.TimeoutError:
            bt.logging.error(
                f"Cycle timed out after {target_timeout}s for {asset} "
                f"{self.prompt_config.label}"
            )
        except asyncio.CancelledError:
            bt.logging.error(f"Cycle cancelled for {asset}")
            raise
        except Exception:
            bt.logging.exception(
                f"Error in {self.prompt_config.label} cycle for {asset}"
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
        first_run: bool = False,
    ) -> int:
        next_cycle = cycle_start_time
        next_cycle = round_time_to_minutes(next_cycle)
        if not first_run:
            next_cycle += timedelta(
                minutes=prompt_config.total_cycle_minutes / len(asset_list)
            )
            next_cycle = next_cycle - timedelta(minutes=1)
        next_cycle_diff = next_cycle - get_current_time()
        delay = int(next_cycle_diff.total_seconds())
        return max(0, delay)

    @staticmethod
    def select_asset(latest_asset: str | None, asset_list: list[str]) -> str:
        if latest_asset is None or latest_asset not in asset_list:
            return asset_list[0]
        latest_index = asset_list.index(latest_asset)
        return asset_list[(latest_index + 1) % len(asset_list)]
