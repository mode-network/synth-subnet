from datetime import datetime, timezone
from functools import lru_cache
import importlib
import sys
import types


@lru_cache(maxsize=1)
def load_scheduler_dependencies():
    bt_stub = types.SimpleNamespace(
        logging=types.SimpleNamespace(
            info=lambda *args, **kwargs: None,
            exception=lambda *args, **kwargs: None,
            warning=lambda *args, **kwargs: None,
        ),
        warning=lambda *args, **kwargs: None,
    )
    sys.modules.setdefault("bittensor", bt_stub)

    miner_data_handler_stub = types.ModuleType(
        "synth.validator.miner_data_handler"
    )
    miner_data_handler_stub.MinerDataHandler = type("MinerDataHandler", (), {})
    sys.modules.setdefault(
        "synth.validator.miner_data_handler", miner_data_handler_stub
    )

    sequential_scheduler = importlib.import_module(
        "synth.utils.sequential_scheduler"
    )
    prompt_config = importlib.import_module("synth.validator.prompt_config")
    return (
        sequential_scheduler.SequentialScheduler,
        prompt_config.HIGH_FREQUENCY,
        prompt_config.LOW_FREQUENCY,
    )


def test_shuffle_assets_same_secret_same_cycle_is_stable():
    SequentialScheduler, _, LOW_FREQUENCY = load_scheduler_dependencies()
    next_run_time = datetime(2026, 4, 19, 12, 1, tzinfo=timezone.utc)

    order_1 = SequentialScheduler.shuffle_assets_for_cycle(
        LOW_FREQUENCY.asset_list,
        next_run_time,
        LOW_FREQUENCY,
        "secret-a",
    )
    order_2 = SequentialScheduler.shuffle_assets_for_cycle(
        LOW_FREQUENCY.asset_list,
        next_run_time,
        LOW_FREQUENCY,
        "secret-a",
    )

    assert order_1 == order_2


def test_shuffle_assets_different_cycle_changes_order():
    SequentialScheduler, _, LOW_FREQUENCY = load_scheduler_dependencies()
    cycle_1 = datetime(2026, 4, 19, 12, 1, tzinfo=timezone.utc)
    cycle_2 = datetime(2026, 4, 19, 13, 1, tzinfo=timezone.utc)

    order_1 = SequentialScheduler.shuffle_assets_for_cycle(
        LOW_FREQUENCY.asset_list,
        cycle_1,
        LOW_FREQUENCY,
        "secret-a",
    )
    order_2 = SequentialScheduler.shuffle_assets_for_cycle(
        LOW_FREQUENCY.asset_list,
        cycle_2,
        LOW_FREQUENCY,
        "secret-a",
    )

    assert order_1 != order_2


def test_shuffle_assets_different_secret_changes_order():
    SequentialScheduler, _, LOW_FREQUENCY = load_scheduler_dependencies()
    next_run_time = datetime(2026, 4, 19, 12, 1, tzinfo=timezone.utc)

    order_1 = SequentialScheduler.shuffle_assets_for_cycle(
        LOW_FREQUENCY.asset_list,
        next_run_time,
        LOW_FREQUENCY,
        "secret-a",
    )
    order_2 = SequentialScheduler.shuffle_assets_for_cycle(
        LOW_FREQUENCY.asset_list,
        next_run_time,
        LOW_FREQUENCY,
        "secret-b",
    )

    assert order_1 != order_2


def test_shuffle_assets_preserves_permutation():
    SequentialScheduler, HIGH_FREQUENCY, _ = load_scheduler_dependencies()
    next_run_time = datetime(2026, 4, 19, 12, 1, tzinfo=timezone.utc)

    shuffled = SequentialScheduler.shuffle_assets_for_cycle(
        HIGH_FREQUENCY.asset_list,
        next_run_time,
        HIGH_FREQUENCY,
        "secret-a",
    )

    assert sorted(shuffled) == sorted(HIGH_FREQUENCY.asset_list)
    assert len(shuffled) == len(HIGH_FREQUENCY.asset_list)
    assert len(set(shuffled)) == len(HIGH_FREQUENCY.asset_list)


def test_select_asset_uses_slot_position_within_cycle():
    SequentialScheduler, _, LOW_FREQUENCY = load_scheduler_dependencies()
    first_slot_time = datetime(2026, 4, 19, 12, 1, tzinfo=timezone.utc)
    second_slot_time = datetime(2026, 4, 19, 12, 6, tzinfo=timezone.utc)
    last_slot_time = datetime(2026, 4, 19, 12, 56, tzinfo=timezone.utc)

    order = SequentialScheduler.shuffle_assets_for_cycle(
        LOW_FREQUENCY.asset_list,
        first_slot_time,
        LOW_FREQUENCY,
        "secret-a",
    )

    assert (
        SequentialScheduler.select_asset(first_slot_time, order, LOW_FREQUENCY)
        == order[0]
    )
    assert (
        SequentialScheduler.select_asset(
            second_slot_time, order, LOW_FREQUENCY
        )
        == order[1]
    )
    assert (
        SequentialScheduler.select_asset(last_slot_time, order, LOW_FREQUENCY)
        == order[-1]
    )
