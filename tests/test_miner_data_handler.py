from datetime import datetime, timedelta

import pytest
from sqlalchemy import Engine, select, delete
from sqlalchemy.dialects.postgresql import insert

from synth.db.models import (
    MinerPrediction,
    MinerScore,
    ValidatorRequest,
    Miner,
)
from synth.validator import response_validation_v2
from synth.validator.prompt_config import PromptConfig
from synth.simulation_input import SimulationInput
from synth.validator.miner_data_handler import MinerDataHandler
from synth.validator.price_data_provider import PriceDataProvider
from synth.validator.reward import get_rewards_multiprocess
from tests.utils import (
    generate_values,
    prepare_random_predictions,
    recent_start_time,
)


@pytest.fixture(scope="function", autouse=True)
def setup_data(db_engine: Engine):
    with db_engine.connect() as connection:
        with connection.begin():
            connection.execute(delete(MinerPrediction))
            connection.execute(delete(ValidatorRequest))


def test_get_values_within_range(db_engine: Engine):
    """
    Test retrieving values within the valid time range.
    2024-11-20T00:00:00       2024-11-20T23:55:00
             |-------------------------|                       (Prediction range)

                                            2024-11-22T00:00:00
                                                    |-|        (Scored Time)
    """
    miner_uids = [10]
    with db_engine.connect() as connection:
        with connection.begin():
            insert_stmt_validator = insert(Miner).values(
                [{"miner_uid": uid} for uid in miner_uids]
            )
            connection.execute(insert_stmt_validator)

    miner_uid = miner_uids[0]
    start_time = "2024-11-20T00:00:00"
    scored_time = datetime.fromisoformat("2024-11-22T00:00:00")
    simulation_input = SimulationInput(
        asset="BTC",
        start_time=start_time,
        time_increment=300,
        time_length=86400,
        num_simulations=1,
    )

    values = generate_values(datetime.fromisoformat(start_time))
    simulation_data = {
        miner_uid: (values, response_validation_v2.CORRECT, "12")
    }
    handler = MinerDataHandler(db_engine)
    handler.save_responses(simulation_data, simulation_input, datetime.now())

    validator_requests = handler.get_validator_requests_to_score(
        scored_time, 7
    )
    assert validator_requests is not None
    assert len(validator_requests) == 1

    result = handler.get_predictions_by_request(int(validator_requests[0].id))

    # get only second element from the result tuple
    # that corresponds to the prediction result
    assert result is not None
    pred = result[0]
    prediction = pred.prediction

    assert len(prediction) == 1
    assert len(prediction[0]) == 288
    assert prediction[0][0] == {"time": "2024-11-20T00:00:00", "price": 90000}
    assert prediction[0][287] == {
        "time": "2024-11-20T23:55:00",
        "price": 233500,
    }


def test_get_values_ongoing_range(db_engine: Engine):
    """
    Test retrieving values when current_time overlaps with the range.
    2024-11-20T00:00:00       2024-11-20T23:55:00
             |-------------------------|         (Prediction range)

                2024-11-20T12:00:00
                        |-|                      (Scored Time)
    """
    miner_uids = [10]
    with db_engine.connect() as connection:
        with connection.begin():
            insert_stmt_validator = insert(Miner).values(
                [{"miner_uid": uid} for uid in miner_uids]
            )
            connection.execute(insert_stmt_validator)

    miner_uid = miner_uids[0]
    start_time = "2024-11-20T00:00:00"
    scored_time = datetime.fromisoformat("2024-11-20T12:00:00")

    simulation_input = SimulationInput(
        asset="BTC",
        start_time=start_time,
        time_increment=300,
        time_length=86400,
        num_simulations=1,
    )

    values = generate_values(datetime.fromisoformat(start_time))
    simulation_data = {
        miner_uid: (values, response_validation_v2.CORRECT, "12")
    }
    handler = MinerDataHandler(db_engine)
    handler.save_responses(simulation_data, simulation_input, datetime.now())

    validator_requests = handler.get_validator_requests_to_score(
        scored_time, 7
    )

    assert validator_requests is not None
    assert len(validator_requests) == 0


def test_multiple_records_for_same_miner(db_engine: Engine):
    """
    Test handling multiple records for the same miner.
    Should take "Prediction range 2" as the latest one

    2024-11-20T00:00:00       2024-11-20T23:55:00
             |-------------------------|                             (Prediction range 1)

                  2024-11-20T12:00:00       2024-11-21T11:55:00
                           |-------------------------|               (Prediction range 2)

                                                  2024-11-21T15:00:00
                                                          |-|        (Current Time)
    """
    miner_uids = [10]
    with db_engine.connect() as connection:
        with connection.begin():
            insert_stmt_validator = insert(Miner).values(
                [{"miner_uid": uid} for uid in miner_uids]
            )
            connection.execute(insert_stmt_validator)

    miner_uid = miner_uids[0]
    start_time_1 = "2024-11-20T00:00:00+00:00"
    start_time_2 = "2024-11-20T12:00:00+00:00"
    scored_time = datetime.fromisoformat("2024-11-21T15:00:00+00:00")

    simulation_input_1 = SimulationInput(
        asset="BTC",
        start_time=start_time_1,
        time_increment=300,
        time_length=86400,
        num_simulations=1,
    )

    simulation_input_2 = SimulationInput(
        asset="BTC",
        start_time=start_time_2,
        time_increment=300,
        time_length=86400,
        num_simulations=1,
    )

    handler = MinerDataHandler(db_engine)

    values_1 = generate_values(datetime.fromisoformat(start_time_1))
    simulation_data_1 = {
        miner_uid: (values_1, response_validation_v2.CORRECT, "12")
    }
    handler.save_responses(
        simulation_data_1, simulation_input_1, datetime.now()
    )

    values_2 = generate_values(datetime.fromisoformat(start_time_2))
    simulation_data_2 = {
        miner_uid: (values_2, response_validation_v2.CORRECT, "12")
    }
    handler.save_responses(
        simulation_data_2, simulation_input_2, datetime.now()
    )

    validator_requests = handler.get_validator_requests_to_score(
        scored_time, 7
    )
    assert validator_requests is not None
    assert len(validator_requests) == 2

    result = handler.get_predictions_by_request(int(validator_requests[1].id))

    assert result is not None
    pred = result[0]
    prediction = pred.prediction

    assert len(prediction) == 1
    assert len(prediction[0]) == 288
    assert prediction[0][0] == {
        "time": "2024-11-20T12:00:00+00:00",
        "price": 90000,
    }
    assert prediction[0][287] == {
        "time": "2024-11-21T11:55:00+00:00",
        "price": 233500,
    }


def test_multiple_records_for_same_miner_with_overlapping(db_engine: Engine):
    """
    Test handling multiple records for the same miner with overlapping records.
    Should take "Prediction range 1" as the latest one

    2024-11-20T00:00:00       2024-11-20T23:55:00
             |-------------------------|                             (Prediction range 1)

                  2024-11-20T12:00:00       2024-11-21T11:55:00
                           |-------------------------|               (Prediction range 2)

                                    2024-11-21T03:00:00
                                            |-|                      (Scored Time)
    """
    miner_uids = [10]
    with db_engine.connect() as connection:
        with connection.begin():
            insert_stmt_validator = insert(Miner).values(
                [{"miner_uid": uid} for uid in miner_uids]
            )
            connection.execute(insert_stmt_validator)

    miner_uid = miner_uids[0]
    start_time_1 = "2024-11-20T00:00:00+00:00"
    start_time_2 = "2024-11-20T12:00:00+00:00"
    scored_time = datetime.fromisoformat("2024-11-21T03:00:00+00:00")

    simulation_input_1 = SimulationInput(
        asset="BTC",
        start_time=start_time_1,
        time_increment=300,
        time_length=86400,
        num_simulations=1,
    )

    simulation_input_2 = SimulationInput(
        asset="BTC",
        start_time=start_time_2,
        time_increment=300,
        time_length=86400,
        num_simulations=1,
    )

    handler = MinerDataHandler(db_engine)

    values_1 = generate_values(datetime.fromisoformat(start_time_1))
    simulation_data_1 = {
        miner_uid: (values_1, response_validation_v2.CORRECT, "12")
    }
    handler.save_responses(
        simulation_data_1, simulation_input_1, datetime.now()
    )

    values_2 = generate_values(datetime.fromisoformat(start_time_2))
    simulation_data_2 = {
        miner_uid: (values_2, response_validation_v2.CORRECT, "12")
    }
    handler.save_responses(
        simulation_data_2, simulation_input_2, datetime.now()
    )

    validator_requests = handler.get_validator_requests_to_score(
        scored_time, 7
    )
    assert validator_requests is not None
    assert len(validator_requests) == 1

    result = handler.get_predictions_by_request(int(validator_requests[0].id))

    # get only second element from the result tuple
    # that corresponds to the prediction result
    assert result is not None
    pred = result[0]
    prediction = pred.prediction

    assert len(prediction) == 1
    assert len(prediction[0]) == 288
    assert prediction[0][0] == {
        "time": "2024-11-20T00:00:00+00:00",
        "price": 90000,
    }
    assert prediction[0][287] == {
        "time": "2024-11-20T23:55:00+00:00",
        "price": 233500,
    }


def test_no_data_for_miner(db_engine: Engine):
    """Test retrieving values for a miner that doesn't exist."""
    scored_time = datetime.fromisoformat("2024-11-20T12:00:00+00:00")

    handler = MinerDataHandler(db_engine)

    validator_requests = handler.get_validator_requests_to_score(
        scored_time, 7
    )
    assert validator_requests is not None
    assert len(validator_requests) == 0


def test_get_values_incorrect_format(db_engine: Engine):
    """
    Test retrieving values within the valid time range.
    2024-11-20T00:00:00       2024-11-20T23:55:00
             |-------------------------|                       (Prediction range)

                                            2024-11-22T00:00:00
                                                    |-|        (Scored Time)
    """
    miner_uids = [10]
    with db_engine.connect() as connection:
        with connection.begin():
            insert_stmt_validator = insert(Miner).values(
                [{"miner_uid": uid} for uid in miner_uids]
            )
            connection.execute(insert_stmt_validator)

    miner_uid = miner_uids[0]
    start_time = "2024-11-20T00:00:00"
    scored_time = datetime.fromisoformat("2024-11-22T00:00:00")
    simulation_input = SimulationInput(
        asset="BTC",
        start_time=start_time,
        time_increment=300,
        time_length=86400,
        num_simulations=1,
    )

    error_string = "some errors in the format"
    simulation_data: dict = {miner_uid: ([], error_string, "12")}
    handler = MinerDataHandler(db_engine)
    handler.save_responses(simulation_data, simulation_input, datetime.now())

    validator_requests = handler.get_validator_requests_to_score(
        scored_time, 7
    )
    assert validator_requests is not None
    assert len(validator_requests) == 1
    result = handler.get_predictions_by_request(int(validator_requests[0].id))

    assert result is not None
    pred = result[0]
    prediction = pred.prediction
    format_validation = pred.format_validation

    assert len(prediction) == 0
    assert format_validation == error_string


def test_set_get_scores(db_engine: Engine):
    handler = MinerDataHandler(db_engine)
    price_data_provider = PriceDataProvider()
    start_time = recent_start_time()
    scored_time = datetime.fromisoformat(start_time) + timedelta(
        hours=24, minutes=5
    )
    handler, _, _ = prepare_random_predictions(db_engine, start_time)

    validator_requests = handler.get_validator_requests_to_score(
        scored_time, 7
    )
    assert validator_requests is not None
    assert len(validator_requests) == 1

    prompt_scores, detailed_info, real_prices = get_rewards_multiprocess(
        handler,
        price_data_provider,
        validator_requests[0],
    )

    assert prompt_scores is not None

    handler.set_miner_scores(
        real_prices, int(validator_requests[0].id), detailed_info, scored_time
    )

    miner_scores_df = handler.get_miner_scores(
        scored_time=scored_time,
        window_days=4,
    )

    print("miner_scores_df", miner_scores_df)


def test_insert_new_miners(db_engine: Engine):
    handler = MinerDataHandler(db_engine)

    with db_engine.connect() as connection:
        with connection.begin():
            initial_len = len(connection.execute(select(Miner)).fetchall())

    handler.insert_new_miners(
        [{"neuron_uid": 111, "coldkey": "coldkey111", "hotkey": "hotkey111"}]
    )

    with db_engine.connect() as connection:
        with connection.begin():
            assert (
                len(connection.execute(select(Miner)).fetchall())
                == initial_len + 1
            )

    handler.insert_new_miners(
        [{"neuron_uid": 111, "coldkey": "coldkey111", "hotkey": "hotkey111"}]
    )

    # Should not insert the same miner again
    with db_engine.connect() as connection:
        with connection.begin():
            assert (
                len(connection.execute(select(Miner)).fetchall())
                == initial_len + 1
            )

    handler.insert_new_miners(
        [
            {
                "neuron_uid": 111,
                "coldkey": "coldkey111-changed",
                "hotkey": "hotkey111-changed",
            }
        ]
    )

    # Should insert new miner with updated values
    with db_engine.connect() as connection:
        with connection.begin():
            assert (
                len(connection.execute(select(Miner)).fetchall())
                == initial_len + 2
            )


def test_set_miner_scores_upsert_preserves_individual_values(
    db_engine: Engine,
):
    """Test that on_conflict_do_update uses each row's own values, not the last row's.

    Previously, the on_conflict_do_update clause referenced a stale loop variable
    `row` which always pointed to the LAST element of reward_details. This meant
    on upsert conflicts, all rows got the last miner's scores.

    Example of the bug:
        reward_details = [
            {"miner_uid": 1, "prompt_score_v3": 0.95, ...},  # miner 1
            {"miner_uid": 2, "prompt_score_v3": 0.30, ...},  # miner 2
        ]
        # After loop: row = miner 2's dict
        # On conflict for miner 1 -> got miner 2's score (0.30) instead of 0.95
    """
    handler = MinerDataHandler(db_engine)
    start_time = "2024-11-20T00:00:00+00:00"
    scored_time = datetime.fromisoformat("2024-11-22T00:00:00+00:00")

    # Setup: create miners and save predictions to get valid miner_predictions_ids
    handler, simulation_input, miner_uids = prepare_random_predictions(
        db_engine, start_time
    )

    validator_requests = handler.get_validator_requests_to_score(
        scored_time, 7
    )
    assert len(validator_requests) >= 1

    # Get prediction IDs for the miners
    with db_engine.connect() as connection:
        predictions = connection.execute(
            select(MinerPrediction.id, MinerPrediction.miner_id).where(
                MinerPrediction.validator_requests_id
                == validator_requests[0].id
            )
        ).fetchall()

    assert len(predictions) >= 2, "Need at least 2 predictions to test upsert"

    # Create reward_details with DIFFERENT scores for each miner
    reward_details = []
    scores = [0.95, 0.30, 0.10, 0.50]  # deliberately different
    for i, pred in enumerate(predictions):
        reward_details.append(
            {
                "miner_uid": i,
                "miner_prediction_id": pred.id,
                "total_crps": scores[i % len(scores)],
                "percentile90": 1.0,
                "lowest_score": 0.01,
                "prompt_score_v3": scores[i % len(scores)],
                "crps_data": [{"crps": scores[i % len(scores)]}],
            }
        )

    # First insert
    handler.set_miner_scores(
        [], int(validator_requests[0].id), reward_details, scored_time
    )

    # Second insert (same prediction IDs -> triggers upsert conflict)
    # Use different scores to verify each row gets its OWN updated values
    updated_reward_details = []
    updated_scores = [0.11, 0.22, 0.33, 0.44]
    for i, pred in enumerate(predictions):
        updated_reward_details.append(
            {
                "miner_uid": i,
                "miner_prediction_id": pred.id,
                "total_crps": updated_scores[i % len(updated_scores)],
                "percentile90": 2.0,
                "lowest_score": 0.02,
                "prompt_score_v3": updated_scores[i % len(updated_scores)],
                "crps_data": [
                    {"crps": updated_scores[i % len(updated_scores)]}
                ],
            }
        )

    handler.set_miner_scores(
        [],
        int(validator_requests[0].id),
        updated_reward_details,
        scored_time,
    )

    # Verify: each miner should have their OWN updated score, not the last
    # miner's
    with db_engine.connect() as connection:
        results = connection.execute(
            select(
                MinerScore.miner_predictions_id,
                MinerScore.prompt_score_v3,
                MinerScore.score_details_v3,
            )
            .where(
                MinerScore.miner_predictions_id.in_(
                    [p.id for p in predictions]
                )
            )
            .order_by(MinerScore.miner_predictions_id)
        ).fetchall()

    assert len(results) == len(predictions)

    for i, result in enumerate(results):
        expected_score = updated_scores[i % len(updated_scores)]
        assert result.prompt_score_v3 == pytest.approx(expected_score), (
            f"Miner {i}: expected prompt_score_v3={expected_score}, "
            f"got {result.prompt_score_v3}. "
            f"Bug: all miners got last miner's score instead of their own."
        )


# ----- prune_redundant_predictions -----------------------------------------

LOW_TEST_CONFIG = PromptConfig(
    asset_list=["BTC"],
    label="low",
    time_length=86400,
    time_increment=300,
    initial_delay=0,
    total_cycle_minutes=60,
    timeout_extra_seconds=60,
    scoring_intervals={},
    window_days=10,
    softmax_beta=-0.1,
    smoothed_score_coefficient=0.5,
    thin_after_minutes=30,
    thin_bucket_seconds=3600,
)

HIGH_TEST_CONFIG = PromptConfig(
    asset_list=["BTC"],
    label="high",
    time_length=3600,
    time_increment=60,
    initial_delay=0,
    total_cycle_minutes=10,
    timeout_extra_seconds=60,
    scoring_intervals={},
    window_days=3,
    softmax_beta=-0.2,
    smoothed_score_coefficient=0.5,
    thin_after_minutes=10,
    thin_bucket_seconds=600,
)


def _insert_miner(connection, miner_uid: int) -> int:
    existing = connection.execute(
        select(Miner.id).where(Miner.miner_uid == miner_uid)
    ).fetchone()
    if existing is not None:
        return int(existing.id)
    result = connection.execute(
        insert(Miner).values(miner_uid=miner_uid).returning(Miner.id)
    )
    return int(result.fetchone().id)


def _insert_prediction(
    connection,
    miner_id: int,
    asset: str,
    time_length: int,
    start_time: datetime,
    created_at: datetime,
    payload: list,
) -> int:
    vr_id = (
        connection.execute(
            insert(ValidatorRequest)
            .values(
                start_time=start_time,
                asset=asset,
                time_increment=300,
                time_length=time_length,
                num_simulations=1,
                request_time=created_at,
            )
            .returning(ValidatorRequest.id)
        )
        .fetchone()
        .id
    )
    mp_id = (
        connection.execute(
            insert(MinerPrediction)
            .values(
                validator_requests_id=vr_id,
                miner_uid=0,
                miner_id=miner_id,
                prediction=payload,
                format_validation=response_validation_v2.CORRECT,
                process_time=1.0,
                created_at=created_at,
            )
            .returning(MinerPrediction.id)
        )
        .fetchone()
        .id
    )
    return int(mp_id)


def _fetch_prediction_state(connection, mp_id: int):
    return connection.execute(
        select(
            MinerPrediction.id,
            MinerPrediction.prediction,
            MinerPrediction.deleted_at,
        ).where(MinerPrediction.id == mp_id)
    ).fetchone()


def test_prune_leaves_recent_requests_untouched(db_engine: Engine):
    """Validator_requests newer than `thin_after_minutes` are not pruned —
    even when many in the same bucket. Short-term density survives for the
    downstream low-latency consumer."""
    now = datetime.now()
    with db_engine.connect() as connection:
        with connection.begin():
            miner_a = _insert_miner(connection, miner_uid=300)
            miner_b = _insert_miner(connection, miner_uid=301)
            ids: list[int] = []
            for m in (2, 5, 10):
                start = now - timedelta(minutes=m)
                for miner_id in (miner_a, miner_b):
                    ids.append(
                        _insert_prediction(
                            connection,
                            miner_id=miner_id,
                            asset="BTC",
                            time_length=LOW_TEST_CONFIG.time_length,
                            start_time=start,
                            created_at=start,
                            payload=[[{"price": float(m)}]],
                        )
                    )

    MinerDataHandler(db_engine).density_tapering_predictions(LOW_TEST_CONFIG)

    with db_engine.connect() as connection:
        for mp_id in ids:
            row = _fetch_prediction_state(connection, mp_id)
            assert row is not None
            assert row.deleted_at is None
            assert isinstance(row.prediction, list)


def test_prune_collapses_old_requests_in_same_bucket(db_engine: Engine):
    """Three old validator_requests for the same asset land in the same
    hour bucket → the smallest-id request is the keeper; every prediction
    on the other two requests gets the `thinned` tombstone, including
    predictions for additional miners on those requests."""
    bucket_anchor = datetime(2026, 1, 1, 12, 0, 0)
    with db_engine.connect() as connection:
        with connection.begin():
            miner_a = _insert_miner(connection, miner_uid=310)
            miner_b = _insert_miner(connection, miner_uid=311)

            # Keeper request — smallest id, both miners.
            kept_a = _insert_prediction(
                connection,
                miner_id=miner_a,
                asset="SOL",
                time_length=LOW_TEST_CONFIG.time_length,
                start_time=bucket_anchor + timedelta(minutes=5),
                created_at=bucket_anchor + timedelta(minutes=5),
                payload=[[{"price": 1.0}]],
            )
            # Extra request 1 in the same bucket — both miners.
            extras: list[int] = []
            for minute in (25, 50):
                start = bucket_anchor + timedelta(minutes=minute)
                for miner_id in (miner_a, miner_b):
                    extras.append(
                        _insert_prediction(
                            connection,
                            miner_id=miner_id,
                            asset="SOL",
                            time_length=LOW_TEST_CONFIG.time_length,
                            start_time=start,
                            created_at=start,
                            payload=[[{"price": float(minute)}]],
                        )
                    )

    MinerDataHandler(db_engine).density_tapering_predictions(LOW_TEST_CONFIG)

    with db_engine.connect() as connection:
        keeper = _fetch_prediction_state(connection, kept_a)
        assert keeper is not None
        assert keeper.deleted_at is None
        assert keeper.prediction == [[{"price": 1.0}]]

        for mp_id in extras:
            row = _fetch_prediction_state(connection, mp_id)
            assert row is not None
            assert row.deleted_at is not None
            assert row.prediction == {"deleted": True, "reason": "thinned"}


def test_prune_keeps_one_request_per_asset_per_bucket(db_engine: Engine):
    """Different assets in the same bucket are independent — each asset
    keeps its smallest-id request. Different buckets are independent —
    each bucket keeps its own request."""
    bucket_anchor = datetime(2026, 1, 1, 12, 0, 0)
    with db_engine.connect() as connection:
        with connection.begin():
            miner_id = _insert_miner(connection, miner_uid=320)
            # bucket 1, BTC: two requests → keep first, prune second
            btc_keep = _insert_prediction(
                connection,
                miner_id=miner_id,
                asset="BTC",
                time_length=LOW_TEST_CONFIG.time_length,
                start_time=bucket_anchor + timedelta(minutes=5),
                created_at=bucket_anchor + timedelta(minutes=5),
                payload=[[{"price": 1.0}]],
            )
            btc_prune = _insert_prediction(
                connection,
                miner_id=miner_id,
                asset="BTC",
                time_length=LOW_TEST_CONFIG.time_length,
                start_time=bucket_anchor + timedelta(minutes=25),
                created_at=bucket_anchor + timedelta(minutes=25),
                payload=[[{"price": 2.0}]],
            )
            # bucket 1, ETH: one request → keep
            eth_keep = _insert_prediction(
                connection,
                miner_id=miner_id,
                asset="ETH",
                time_length=LOW_TEST_CONFIG.time_length,
                start_time=bucket_anchor + timedelta(minutes=10),
                created_at=bucket_anchor + timedelta(minutes=10),
                payload=[[{"price": 3.0}]],
            )
            # bucket 2, BTC: one request → keep
            btc_next_bucket = _insert_prediction(
                connection,
                miner_id=miner_id,
                asset="BTC",
                time_length=LOW_TEST_CONFIG.time_length,
                start_time=bucket_anchor + timedelta(hours=1, minutes=5),
                created_at=bucket_anchor + timedelta(hours=1, minutes=5),
                payload=[[{"price": 4.0}]],
            )

    MinerDataHandler(db_engine).density_tapering_predictions(LOW_TEST_CONFIG)

    with db_engine.connect() as connection:
        assert _fetch_prediction_state(connection, btc_keep).deleted_at is None
        assert (
            _fetch_prediction_state(connection, btc_prune).deleted_at
            is not None
        )
        assert _fetch_prediction_state(connection, eth_keep).deleted_at is None
        assert (
            _fetch_prediction_state(connection, btc_next_bucket).deleted_at
            is None
        )


def test_prune_scopes_by_time_length(db_engine: Engine):
    """LOW pruning must not touch HIGH validator_requests, and vice versa."""
    bucket_anchor = datetime(2026, 1, 1, 12, 0, 0)
    with db_engine.connect() as connection:
        with connection.begin():
            miner_id = _insert_miner(connection, miner_uid=330)
            low_kept = _insert_prediction(
                connection,
                miner_id=miner_id,
                asset="ETH",
                time_length=LOW_TEST_CONFIG.time_length,
                start_time=bucket_anchor,
                created_at=bucket_anchor,
                payload=[[{"price": 1.0}]],
            )
            low_extra = _insert_prediction(
                connection,
                miner_id=miner_id,
                asset="ETH",
                time_length=LOW_TEST_CONFIG.time_length,
                start_time=bucket_anchor + timedelta(minutes=30),
                created_at=bucket_anchor + timedelta(minutes=30),
                payload=[[{"price": 2.0}]],
            )
            high_a = _insert_prediction(
                connection,
                miner_id=miner_id,
                asset="ETH",
                time_length=HIGH_TEST_CONFIG.time_length,
                start_time=bucket_anchor,
                created_at=bucket_anchor,
                payload=[[{"price": 9.0}]],
            )
            high_b = _insert_prediction(
                connection,
                miner_id=miner_id,
                asset="ETH",
                time_length=HIGH_TEST_CONFIG.time_length,
                start_time=bucket_anchor + timedelta(minutes=4),
                created_at=bucket_anchor + timedelta(minutes=4),
                payload=[[{"price": 8.0}]],
            )

    MinerDataHandler(db_engine).density_tapering_predictions(LOW_TEST_CONFIG)

    with db_engine.connect() as connection:
        assert _fetch_prediction_state(connection, low_kept).deleted_at is None
        assert (
            _fetch_prediction_state(connection, low_extra).deleted_at
            is not None
        )
        # HIGH untouched.
        assert _fetch_prediction_state(connection, high_a).deleted_at is None
        assert _fetch_prediction_state(connection, high_b).deleted_at is None


def test_prune_high_bucket_is_ten_minutes(db_engine: Engine):
    """HIGH bucket = 600 s. Two requests inside the same 10-min bucket
    collapse to one; an adjacent-bucket request survives."""
    bucket_anchor = datetime(2026, 1, 1, 11, 0, 0)
    with db_engine.connect() as connection:
        with connection.begin():
            miner_id = _insert_miner(connection, miner_uid=340)
            kept_a = _insert_prediction(
                connection,
                miner_id=miner_id,
                asset="HYPE",
                time_length=HIGH_TEST_CONFIG.time_length,
                start_time=bucket_anchor,
                created_at=bucket_anchor,
                payload=[[{"price": 1.0}]],
            )
            extra_a = _insert_prediction(
                connection,
                miner_id=miner_id,
                asset="HYPE",
                time_length=HIGH_TEST_CONFIG.time_length,
                start_time=bucket_anchor + timedelta(minutes=4),
                created_at=bucket_anchor + timedelta(minutes=4),
                payload=[[{"price": 2.0}]],
            )
            kept_b = _insert_prediction(
                connection,
                miner_id=miner_id,
                asset="HYPE",
                time_length=HIGH_TEST_CONFIG.time_length,
                start_time=bucket_anchor + timedelta(minutes=12),
                created_at=bucket_anchor + timedelta(minutes=12),
                payload=[[{"price": 3.0}]],
            )

    MinerDataHandler(db_engine).density_tapering_predictions(HIGH_TEST_CONFIG)

    with db_engine.connect() as connection:
        assert _fetch_prediction_state(connection, kept_a).deleted_at is None
        assert (
            _fetch_prediction_state(connection, extra_a).deleted_at is not None
        )
        assert _fetch_prediction_state(connection, kept_b).deleted_at is None
