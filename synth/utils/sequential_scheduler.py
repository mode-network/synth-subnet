from datetime import datetime, timedelta, timezone
import hashlib
import os
import secrets
import time


import bittensor as bt


from synth.validator.miner_data_handler import MinerDataHandler
from synth.validator.prompt_config import PromptConfig
from synth.utils.helpers import get_current_time, round_time_to_minutes


class SequentialScheduler:
    def __init__(
        self,
        prompt_config: PromptConfig,
        target: callable,
        miner_data_handler: MinerDataHandler,
    ):
        self.prompt_config = prompt_config
        self.target = target
        self.miner_data_handler = miner_data_handler
        self.first_run = True
        self.schedule_secret = (
            os.getenv("VALIDATOR_SCHEDULE_SECRET") or secrets.token_hex(32)
        )

    def start(self):
        cycle_start_time = get_current_time()
        while True:
            try:
                cycle_start_time = self.run_cycle(cycle_start_time)
            except Exception:
                bt.logging.exception("Error in cycle ")
                cycle_start_time = get_current_time()
            self.first_run = False

    def run_cycle(
        self,
        cycle_start_time: datetime,
    ):
        prompt_config = self.prompt_config

        asset_list = prompt_config.asset_list
        if get_current_time() <= datetime(
            2026, 4, 10, 14, 0, 0, tzinfo=timezone.utc
        ):
            if prompt_config.label == "low":
                asset_list = prompt_config.asset_list[:9]
            elif prompt_config.label == "high":
                asset_list = prompt_config.asset_list[:4]

        next_run_time = self.select_next_run_time(
            asset_list,
            cycle_start_time,
            prompt_config,
            self.first_run,
        )
        delay = self.select_delay(next_run_time)
        asset_order = self.shuffle_assets_for_cycle(
            asset_list,
            next_run_time,
            prompt_config,
            self.schedule_secret,
        )
        asset = self.select_asset(next_run_time, asset_order, prompt_config)

        bt.logging.info(
            f"Scheduling next {prompt_config.label} frequency cycle for asset {asset} in {delay} seconds"
        )

        time.sleep(delay)
        cycle_start_time = get_current_time()
        self.target(asset)

        return cycle_start_time

    @staticmethod
    def select_next_run_time(
        asset_list: list[str],
        cycle_start_time: datetime,
        prompt_config: PromptConfig,
        first_run: bool = False,
    ) -> datetime:
        next_cycle = cycle_start_time
        next_cycle = round_time_to_minutes(next_cycle)
        if not first_run:
            next_cycle += timedelta(
                minutes=prompt_config.total_cycle_minutes / len(asset_list)
            )
            next_cycle = next_cycle - timedelta(minutes=1)

        return next_cycle

    @staticmethod
    def select_delay(next_cycle: datetime) -> int:
        next_cycle_diff = next_cycle - get_current_time()
        delay = int(next_cycle_diff.total_seconds())
        if delay <= 0:
            bt.warning("Calculated delay is non-positive")
            current_time = get_current_time()
            diff = round_time_to_minutes(current_time) - current_time
            delay = int(diff.total_seconds())

        return delay

    @staticmethod
    def get_cycle_start_time(
        next_run_time: datetime, prompt_config: PromptConfig
    ) -> datetime:
        total_cycle_minutes = prompt_config.total_cycle_minutes
        cycle_minute = (
            next_run_time.minute // total_cycle_minutes
        ) * total_cycle_minutes
        return next_run_time.replace(
            minute=cycle_minute, second=0, microsecond=0
        )

    @staticmethod
    def shuffle_assets_for_cycle(
        asset_list: list[str],
        next_run_time: datetime,
        prompt_config: PromptConfig,
        schedule_secret: str,
    ) -> list[str]:
        cycle_start_time = SequentialScheduler.get_cycle_start_time(
            next_run_time, prompt_config
        )
        cycle_key = cycle_start_time.astimezone(timezone.utc).isoformat()
        keyed_assets = [
            (
                hashlib.sha256(
                    f"{schedule_secret}:{cycle_key}:{asset}".encode("utf-8")
                ).hexdigest(),
                asset,
            )
            for asset in asset_list
        ]
        return [asset for _, asset in sorted(keyed_assets)]

    @staticmethod
    def select_asset(
        next_run_time: datetime,
        asset_list: list[str],
        prompt_config: PromptConfig,
    ) -> str:
        cycle_start_time = SequentialScheduler.get_cycle_start_time(
            next_run_time, prompt_config
        )
        slot_seconds = (
            prompt_config.total_cycle_minutes / len(asset_list)
        ) * 60
        elapsed_seconds = (next_run_time - cycle_start_time).total_seconds()
        slot_index = int(elapsed_seconds // slot_seconds) % len(asset_list)

        return asset_list[slot_index]
