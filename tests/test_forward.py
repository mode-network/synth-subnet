from synth.validator.forward import (
    _calculate_moving_average_and_update_rewards,
    _calculate_rewards_and_update_scores,
)
from synth.validator.price_data_provider import PriceDataProvider
from tests.utils import prepare_random_predictions


def test_calculate_rewards_and_update_scores(db_engine):
    start_time = "2024-11-26T00:00:00+00:00"
    scored_time = "2024-11-28T00:00:00+00:00"

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
    scored_time = "2024-11-28T00:00:00+00:00"

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
