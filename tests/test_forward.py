from datetime import datetime, timedelta, timezone
import logging


from numpy.testing import assert_almost_equal
import bittensor as bt


from sqlalchemy import Engine, insert, select
from synth.miner.simulations import generate_simulations
from synth.simulation_input import SimulationInput
from synth.validator import response_validation_v2
from synth.validator.forward import (
    calculate_moving_average_and_update_rewards,
    calculate_rewards_and_update_scores,
)
from synth.db.models import Miner, MinerReward
from synth.validator.miner_data_handler import MinerDataHandler
from synth.validator.price_data_provider import PriceDataProvider
from tests.utils import prepare_random_predictions


def test_calculate_rewards_and_update_scores(db_engine: Engine):
    start_time = "2024-08-25T23:58:00+00:00"
    scored_time = datetime.fromisoformat("2024-08-28T00:00:00+00:00")

    handler, _, miner_uids = prepare_random_predictions(db_engine, start_time)

    price_data_provider = PriceDataProvider()

    success = calculate_rewards_and_update_scores(
        miner_data_handler=handler,
        price_data_provider=price_data_provider,
        scored_time=scored_time,
        cutoff_days=7,
    )

    assert success

    miner_scores_df = handler.get_miner_scores(
        scored_time=scored_time,
        cutoff_days=2,
    )

    assert len(miner_scores_df) == len(miner_uids)

    print("miner_scores_df", miner_scores_df)


def test_calculate_moving_average_and_update_rewards(db_engine: Engine):
    start_time = "2024-09-25T23:58:00+00:00"
    scored_time = datetime.fromisoformat("2024-09-28T00:00:00+00:00")

    handler, _, _ = prepare_random_predictions(db_engine, start_time)

    price_data_provider = PriceDataProvider()

    success = calculate_rewards_and_update_scores(
        miner_data_handler=handler,
        price_data_provider=price_data_provider,
        scored_time=scored_time,
        cutoff_days=7,
    )

    assert success

    moving_averages_data = calculate_moving_average_and_update_rewards(
        miner_data_handler=handler,
        scored_time=scored_time,
        cutoff_days=4,
        window_days=2,
        softmax_beta=-0.003,
    )

    print("moving_averages_data", moving_averages_data)


def test_calculate_moving_average_and_update_rewards_new_miner(
    db_engine: Engine,
):
    miner_uids = [10, 20, 33, 40, 50, 60]
    with db_engine.connect() as connection:
        with connection.begin():
            insert_stmt_validator = insert(Miner).values(
                [{"miner_uid": uid} for uid in miner_uids]
            )
            connection.execute(insert_stmt_validator)

    handler = MinerDataHandler(db_engine)
    start_time_str = "2024-10-25T23:58:00+00:00"
    num_predictions = 6
    for i in range(num_predictions):
        miner_uids = [10, 20, 33, 40, 50, 60]
        start_time = datetime.fromisoformat(start_time_str).replace(
            tzinfo=timezone.utc
        ) + timedelta(hours=i)
        start_time_str = start_time.isoformat()
        simulation_input = SimulationInput(
            asset="BTC",
            start_time=start_time_str,
            time_increment=300,
            time_length=86400,
            num_simulations=1,
        )

        simulation_data = {
            miner_uids[0]: (
                generate_simulations(start_time=start_time_str),
                response_validation_v2.CORRECT,
                "1.2",
            ),
            miner_uids[1]: (
                generate_simulations(start_time=start_time_str),
                response_validation_v2.CORRECT,
                "3",
            ),
            miner_uids[2]: (
                generate_simulations(start_time=start_time_str),
                response_validation_v2.CORRECT,
                "15",
            ),
            miner_uids[3]: (
                generate_simulations(start_time=start_time_str),
                "time out or internal server error (process time is None)",
                "2.1",
            ),
            miner_uids[4]: (
                generate_simulations(start_time=start_time_str),
                response_validation_v2.CORRECT,
                "1.5",
            ),
            miner_uids[5]: (
                generate_simulations(start_time=start_time_str),
                response_validation_v2.CORRECT,
                "5",
            ),
        }

        # simulate a miner that join later the subnet
        if i < 2:
            del simulation_data[miner_uids[5]]
            del miner_uids[5]

        handler.save_responses(
            simulation_data, simulation_input, datetime.now()
        )

        price_data_provider = PriceDataProvider()

        # scored time is start time + 24 hours and +4 minutes because new prompt every 64 minutes
        scored_time = start_time + timedelta(days=1, minutes=4)

        success = calculate_rewards_and_update_scores(
            miner_data_handler=handler,
            price_data_provider=price_data_provider,
            scored_time=scored_time,
            cutoff_days=7,
        )

        miner_scores_df = handler.get_miner_scores(
            scored_time=scored_time,
            cutoff_days=4,
        )

        print("miner_scores_df", miner_scores_df)

        assert success

        moving_averages_data = calculate_moving_average_and_update_rewards(
            miner_data_handler=handler,
            scored_time=scored_time,
            cutoff_days=4,
            window_days=2,
            softmax_beta=-0.003,
        )

        print("moving_averages_data", moving_averages_data)


def test_calculate_moving_average_and_update_rewards_new_miner_registration(
    db_engine: Engine,
):
    bt.logging._logger.setLevel(logging.DEBUG)
    miner_uids = [10, 20, 33, 40, 50, 60]
    with db_engine.connect() as connection:
        with connection.begin():
            records = []
            for uid in miner_uids:
                records.append(
                    {
                        "miner_uid": uid,
                        "coldkey": "5c" + str(uid),
                        "hotkey": "5h" + str(uid),
                    }
                )

            insert_stmt_validator = insert(Miner).values(records)
            connection.execute(insert_stmt_validator)

    handler = MinerDataHandler(db_engine)
    start_time_str = "2024-11-25T23:58:00+00:00"
    num_predictions = 6
    for i in range(num_predictions):
        print("I is ", i)
        miner_uids = [10, 20, 33, 40, 50, 60]
        start_time = datetime.fromisoformat(start_time_str).replace(
            tzinfo=timezone.utc
        ) + timedelta(hours=i)
        start_time_str = start_time.isoformat()
        simulation_input = SimulationInput(
            asset="BTC" if i % 2 == 0 else "ETH",
            start_time=start_time_str,
            time_increment=300,
            time_length=86400,
            num_simulations=1,
        )

        simulation_data = {
            miner_uids[0]: (
                generate_simulations(start_time=start_time_str),
                response_validation_v2.CORRECT,
                "1.2",
            ),
            miner_uids[1]: (
                generate_simulations(start_time=start_time_str),
                response_validation_v2.CORRECT,
                "3",
            ),
            miner_uids[2]: (
                generate_simulations(start_time=start_time_str),
                response_validation_v2.CORRECT,
                "15",
            ),
            miner_uids[3]: (
                generate_simulations(start_time=start_time_str),
                "time out or internal server error (process time is None)",
                "2.1",
            ),
            miner_uids[4]: (
                generate_simulations(start_time=start_time_str),
                response_validation_v2.CORRECT,
                "1.5",
            ),
            miner_uids[5]: (
                generate_simulations(start_time=start_time_str),
                response_validation_v2.CORRECT,
                "5",
            ),
        }

        # simulate a miner that join later the subnet
        if i < 2:
            del simulation_data[miner_uids[5]]
            del miner_uids[5]

        # simulate a new miner registration
        if i == 3:
            with db_engine.connect() as connection:
                with connection.begin():
                    insert_stmt_validator = insert(Miner).values(
                        [
                            {
                                "miner_uid": miner_uids[0],
                                "coldkey": "5cNew" + str(uid),
                                "hotkey": "5hNew" + str(uid),
                            }
                        ]
                    )
                    connection.execute(insert_stmt_validator)

        handler.save_responses(
            simulation_data, simulation_input, datetime.now()
        )

        price_data_provider = PriceDataProvider()

        # scored time is start time + 24 hours and +4 minutes because new prompt every 64 minutes
        scored_time = start_time + timedelta(days=1, minutes=4)

        success = calculate_rewards_and_update_scores(
            miner_data_handler=handler,
            price_data_provider=price_data_provider,
            scored_time=scored_time,
            cutoff_days=7,
        )

        miner_scores_df = handler.get_miner_scores(
            scored_time=scored_time,
            cutoff_days=4,
        )

        print("miner_scores_df: ", miner_scores_df)

        assert success

        moving_averages_data = calculate_moving_average_and_update_rewards(
            miner_data_handler=handler,
            scored_time=scored_time,
            cutoff_days=4,
            window_days=2,
            softmax_beta=-0.003,
        )

        print("moving_averages_data", moving_averages_data)

        # sum the reward weights
        with db_engine.connect() as connection:
            with connection.begin():
                rewards_rows_select = select(MinerReward).where(
                    MinerReward.updated_at == scored_time
                )
                rewards_rows = connection.execute(rewards_rows_select).all()
                print("rewards_rows", rewards_rows)
                rewards_sum = sum([row.reward_weight for row in rewards_rows])
                print("rewards_sum", rewards_sum)

        miner_weights = [
            item["reward_weight"] for item in moving_averages_data
        ]
        assert_almost_equal(sum(miner_weights), 1, decimal=12)


def test_calculate_moving_average_and_update_rewards_only_invalid(
    db_engine: Engine,
):
    handler = MinerDataHandler(db_engine)
    start_time_str = "2024-12-28T23:58:00+00:00"

    handler.update_miner_rewards(
        [
            {
                "miner_uid": 0,
                "smoothed_score": float("nan"),
                "reward_weight": float("nan"),
                "updated_at": "2024-11-25T21:00:00+00:00",
            },
            {
                "miner_uid": 1,
                "smoothed_score": float("nan"),
                "reward_weight": float("nan"),
                "updated_at": "2024-11-25T21:00:00+00:00",
            },
            {
                "miner_uid": 2,
                "smoothed_score": float("nan"),
                "reward_weight": float("nan"),
                "updated_at": "2024-11-25T21:00:00+00:00",
            },
            {
                "miner_uid": 3,
                "smoothed_score": float("nan"),
                "reward_weight": float("nan"),
                "updated_at": "2024-11-25T21:00:00+00:00",
            },
        ]
    )

    num_predictions = 3
    for i in range(num_predictions):
        miner_uids = [0, 1, 2, 3, 4, 5]
        start_time = datetime.fromisoformat(start_time_str).replace(
            tzinfo=timezone.utc
        ) + timedelta(hours=i)
        start_time_str = start_time.isoformat()
        simulation_input = SimulationInput(
            asset="BTC",
            start_time=start_time_str,
            time_increment=300,
            time_length=86400,
            num_simulations=1,
        )

        simulation_data = {
            miner_uids[0]: (
                [],
                "time out or internal server error (process time is None)",
                "1.2",
            ),
            miner_uids[1]: (
                [],
                "time out or internal server error (process time is None)",
                "3",
            ),
            miner_uids[2]: (
                generate_simulations(start_time=start_time_str),
                response_validation_v2.CORRECT,
                "15",
            ),
        }

        handler.save_responses(
            simulation_data, simulation_input, datetime.now()
        )

        price_data_provider = PriceDataProvider()

        # scored time is start time + 24 hours and +4 minutes because new prompt every 64 minutes
        scored_time = start_time + timedelta(days=1, minutes=4)

        success = calculate_rewards_and_update_scores(
            miner_data_handler=handler,
            price_data_provider=price_data_provider,
            scored_time=scored_time,
            cutoff_days=7,
        )

        miner_scores_df = handler.get_miner_scores(
            scored_time=scored_time,
            cutoff_days=4,
        )

        print("miner_scores_df", miner_scores_df)

        assert success

        moving_averages_data = calculate_moving_average_and_update_rewards(
            miner_data_handler=handler,
            scored_time=scored_time,
            cutoff_days=4,
            window_days=2,
            softmax_beta=-0.003,
        )

        print("moving_averages_data", moving_averages_data)
