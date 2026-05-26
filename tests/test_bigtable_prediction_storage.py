"""Unit tests for the Bigtable prediction storage backend.

Bigtable I/O is mocked: we verify routing, serialization, and skip-logic but
not the actual google-cloud-bigtable client behaviour.
"""

from unittest.mock import MagicMock

import numpy as np
import pytest

pytest.importorskip("google.cloud.bigtable")

from synth.simulation_input import SimulationInput  # noqa: E402
from synth.validator import prompt_config  # noqa: E402
from synth.validator import response_validation_v2  # noqa: E402
from synth.validator import bigtable_prediction_storage as bps  # noqa: E402

CORRECT = response_validation_v2.CORRECT
LOW_TIME_LENGTH = prompt_config.LOW_FREQUENCY.time_length
LOW_TIME_INCREMENT = prompt_config.LOW_FREQUENCY.time_increment
HIGH_TIME_LENGTH = prompt_config.HIGH_FREQUENCY.time_length
HIGH_TIME_INCREMENT = prompt_config.HIGH_FREQUENCY.time_increment


def _make_production_prediction(num_simulations: int, num_timesteps: int):
    """Build a prediction in production wire format.

    `[start_ts, time_increment, path1, path2, ...]` where each path is a
    list of `num_timesteps` floats.
    """
    rng = np.random.default_rng(0)
    paths = rng.uniform(
        50_000, 100_000, size=(num_simulations, num_timesteps)
    ).astype(np.float32)
    return [1700000000, 300, *paths.tolist()]


def _make_storage_with_mock_tables():
    """Bypass __init__ so we don't need real env vars."""
    storage = bps.BigtablePredictionStorage.__new__(
        bps.BigtablePredictionStorage
    )
    storage._table_low_id = "tbl_low"
    storage._table_high_id = "tbl_high"
    storage._tables = {"low": MagicMock(), "high": MagicMock()}
    return storage


def test_build_row_key_format():
    key = bps.BigtablePredictionStorage.build_row_key(
        "BTC", "low", "2026-05-25T12:00:00", 42
    )
    assert key == "BTC#low#2026-05-25T12:00:00#42"


def test_paths_round_trip():
    prediction = _make_production_prediction(
        num_simulations=4, num_timesteps=7
    )
    blob = bps._paths_to_float32_bytes(prediction)

    paths = bps._float32_bytes_to_paths(
        blob, num_simulations=4, num_timesteps=7
    )

    expected = np.asarray(prediction[2:], dtype=np.float32).tolist()
    assert paths == expected


def _low_sim_input():
    return SimulationInput(
        asset="BTC",
        start_time="2026-05-25T12:00:00",
        time_increment=LOW_TIME_INCREMENT,
        time_length=LOW_TIME_LENGTH,
        num_simulations=2,
    )


def _high_sim_input():
    return SimulationInput(
        asset="ETH",
        start_time="2026-05-25T12:00:00",
        time_increment=HIGH_TIME_INCREMENT,
        time_length=HIGH_TIME_LENGTH,
        num_simulations=2,
    )


def _validator_request(time_length, time_increment, num_simulations):
    return MagicMock(
        time_length=time_length,
        time_increment=time_increment,
        num_simulations=num_simulations,
    )


def test_write_predictions_skips_invalid_format_and_unknown_miners():
    storage = _make_storage_with_mock_tables()
    storage._tables["low"].direct_row.side_effect = lambda key: MagicMock(
        key=key
    )
    storage._tables["low"].mutate_rows.return_value = []

    good = _make_production_prediction(2, 3)
    bad = _make_production_prediction(2, 3)
    sim_input = _low_sim_input()
    miner_predictions = {
        10: (good, CORRECT, "1.0"),
        11: (bad, "time out or internal server error", "1.5"),
        # uid 12 is not in miner_id_map below — should be skipped
        12: (good, CORRECT, "1.0"),
    }
    miner_id_map = {10: 100, 11: 101}

    keys = storage.write_predictions(
        simulation_input=sim_input,
        miner_predictions=miner_predictions,
        miner_id_map=miner_id_map,
    )

    # only miner 10 was CORRECT *and* in miner_id_map
    assert list(keys.keys()) == [10]
    assert keys[10] == "BTC#low#2026-05-25T12:00:00#100"
    storage._tables["high"].mutate_rows.assert_not_called()
    assert storage._tables["low"].mutate_rows.call_count == 1


def test_write_predictions_routes_to_high_table():
    storage = _make_storage_with_mock_tables()
    storage._tables["high"].direct_row.side_effect = lambda key: MagicMock(
        key=key
    )
    storage._tables["high"].mutate_rows.return_value = []

    prediction = _make_production_prediction(2, 3)
    sim_input = _high_sim_input()
    storage.write_predictions(
        simulation_input=sim_input,
        miner_predictions={10: (prediction, CORRECT, "0.9")},
        miner_id_map={10: 100},
    )

    storage._tables["low"].mutate_rows.assert_not_called()
    storage._tables["high"].mutate_rows.assert_called_once()


def test_read_predictions_missing_rows_return_empty():
    storage = _make_storage_with_mock_tables()
    # Bigtable returns no rows — every key should map to [].
    storage._tables["low"].read_rows.return_value = iter([])
    vr = _validator_request(LOW_TIME_LENGTH, LOW_TIME_INCREMENT, 2)

    result = storage.read_predictions(vr, ["BTC#low#t0#100", "BTC#low#t0#101"])

    assert result == {"BTC#low#t0#100": [], "BTC#low#t0#101": []}


def test_read_predictions_decodes_cell_bytes():
    storage = _make_storage_with_mock_tables()
    # use a tiny shape so the prediction fixture fits the validator_request
    num_sims, num_steps = 2, 3
    prediction = _make_production_prediction(num_sims, num_steps)
    blob = bps._paths_to_float32_bytes(prediction)

    cell = MagicMock()
    cell.value = blob
    bt_row = MagicMock()
    bt_row.row_key = b"BTC#low#t0#100"
    bt_row.cells = {bps.COLUMN_FAMILY: {bps.COLUMN_QUALIFIER: [cell]}}
    storage._tables["low"].read_rows.return_value = iter([bt_row])

    # validator_request.time_length / time_increment + 1 must equal num_steps
    # so the reshape lines up; pick increment=1, length=num_steps-1.
    vr = _validator_request(num_steps - 1, 1, num_sims)
    result = storage.read_predictions(vr, ["BTC#low#t0#100"])

    expected = np.asarray(prediction[2:], dtype=np.float32).tolist()
    assert result["BTC#low#t0#100"] == expected
