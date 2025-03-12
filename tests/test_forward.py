from datetime import datetime, timedelta, timezone
from synth.miner.simulations import generate_simulations
from synth.simulation_input import SimulationInput
from synth.validator import response_validation
from synth.validator.forward import (
    _calculate_moving_average_and_update_rewards,
    _calculate_rewards_and_update_scores,
)
from synth.validator.miner_data_handler import MinerDataHandler
from synth.validator.price_data_provider import PriceDataProvider
from tests.utils import prepare_random_predictions


def test_calculate_rewards_and_update_scores(db_engine):
    start_time = "2024-11-26T00:00:00+00:00"
    scored_time = "2024-11-27T00:04:00+00:00"

    handler, simulation_input, miner_uids = prepare_random_predictions(
        db_engine, start_time
    )

    price_data_provider = PriceDataProvider("BTC")

    success = _calculate_rewards_and_update_scores(
        miner_data_handler=handler,
        miner_uids=miner_uids,
        price_data_provider=price_data_provider,
        scored_time=scored_time,
        simulation_input=simulation_input,
    )

    assert success

    miner_scores_df = handler.get_miner_scores(
        scored_time_str=scored_time,
        cutoff_days=2,
    )

    assert len(miner_scores_df) == len(miner_uids)

    print(miner_scores_df)
    # print(miner_scores_df['score_details_v2'][0])


def test_calculate_moving_average_and_update_rewards(db_engine):
    start_time = "2024-11-26T00:00:00+00:00"
    scored_time = "2024-11-27T00:04:00+00:00"

    handler, simulation_input, miner_uids = prepare_random_predictions(
        db_engine, start_time
    )

    price_data_provider = PriceDataProvider("BTC")

    success = _calculate_rewards_and_update_scores(
        miner_data_handler=handler,
        miner_uids=miner_uids,
        price_data_provider=price_data_provider,
        scored_time=scored_time,
        simulation_input=simulation_input,
    )

    assert success

    filtered_miner_uids, filtered_rewards = (
        _calculate_moving_average_and_update_rewards(
            miner_data_handler=handler,
            scored_time=scored_time,
            cutoff_days=4,
            half_life_days=2,
            softmax_beta=-0.003,
        )
    )

    print(filtered_miner_uids)
    print(filtered_rewards)


def test_calculate_moving_average_and_update_rewards_new_miner(db_engine):
    handler = MinerDataHandler(db_engine)
    start_time_str = "2024-11-26T00:00:00+00:00"
    num_predictions = 6
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
                generate_simulations(start_time=start_time_str),
                response_validation.CORRECT,
                "1.2",
            ),
            miner_uids[1]: (
                generate_simulations(start_time=start_time_str),
                response_validation.CORRECT,
                "3",
            ),
            miner_uids[2]: (
                generate_simulations(start_time=start_time_str),
                response_validation.CORRECT,
                "15",
            ),
            miner_uids[3]: (
                generate_simulations(start_time=start_time_str),
                "time out or internal server error (process time is None)",
                "2.1",
            ),
            miner_uids[4]: (
                generate_simulations(start_time=start_time_str),
                response_validation.CORRECT,
                "1.5",
            ),
            miner_uids[5]: (
                generate_simulations(start_time=start_time_str),
                response_validation.CORRECT,
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

        price_data_provider = PriceDataProvider("BTC")

        # scored time is start time + 24 hours and +4 minutes because new prompt every 64 minutes
        scored_time = start_time + timedelta(days=1, minutes=4)

        success = _calculate_rewards_and_update_scores(
            miner_data_handler=handler,
            miner_uids=miner_uids,
            price_data_provider=price_data_provider,
            scored_time=scored_time.isoformat(),
            simulation_input=simulation_input,
        )

        miner_scores_df = handler.get_miner_scores(
            scored_time_str=scored_time.isoformat(),
            cutoff_days=4,
        )

        print(miner_scores_df)

        assert success

        filtered_miner_uids, filtered_rewards = (
            _calculate_moving_average_and_update_rewards(
                miner_data_handler=handler,
                scored_time=scored_time.isoformat(),
                cutoff_days=4,
                half_life_days=2,
                softmax_beta=-0.003,
            )
        )

        print(filtered_miner_uids)
        print(filtered_rewards)


def test_calculate_moving_average_and_update_rewards_only_invalid(db_engine):
    handler = MinerDataHandler(db_engine)
    start_time_str = "2024-11-25T23:58:00+00:00"

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
                response_validation.CORRECT,
                "15",
            ),
        }

        handler.save_responses(
            simulation_data, simulation_input, datetime.now()
        )

        price_data_provider = PriceDataProvider("BTC")

        # scored time is start time + 24 hours and +4 minutes because new prompt every 64 minutes
        scored_time = start_time + timedelta(days=1, minutes=4)

        success = _calculate_rewards_and_update_scores(
            miner_data_handler=handler,
            miner_uids=miner_uids,
            price_data_provider=price_data_provider,
            scored_time=scored_time.isoformat(),
            simulation_input=simulation_input,
        )

        miner_scores_df = handler.get_miner_scores(
            scored_time_str=scored_time.isoformat(),
            cutoff_days=4,
        )

        print(miner_scores_df)

        assert success

        filtered_miner_uids, filtered_rewards = (
            _calculate_moving_average_and_update_rewards(
                miner_data_handler=handler,
                scored_time=scored_time.isoformat(),
                cutoff_days=4,
                half_life_days=2,
                softmax_beta=-0.003,
            )
        )

        print(filtered_miner_uids)
        print(filtered_rewards)
