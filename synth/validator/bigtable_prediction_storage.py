"""Bigtable storage backend for miner predictions.

Predictions for a single (asset, prompt_label, start_time, miner) live in one
Bigtable row, value = raw float32 bytes (num_simulations x num_timesteps).
Two tables are used so that retention can be enforced by per-table GC policy:
one for the `low` prompt and one for the `high` prompt.

The Postgres `miner_predictions` row stays — its `prediction` column holds a
sentinel JSON, and `bigtable_key` holds the row key used here.
"""

from __future__ import annotations

import os

import bittensor as bt
import numpy as np
from google.cloud import bigtable
from google.cloud.bigtable.row_filters import CellsColumnLimitFilter
from google.cloud.bigtable.row_set import RowSet

from synth.simulation_input import SimulationInput
from synth.validator import response_validation_v2

COLUMN_FAMILY = "p"
COLUMN_QUALIFIER = b"d"

_ENV_PROJECT = "BIGTABLE_PROJECT"
_ENV_INSTANCE = "BIGTABLE_INSTANCE"
_ENV_TABLE_LOW = "BIGTABLE_TABLE_LOW"
_ENV_TABLE_HIGH = "BIGTABLE_TABLE_HIGH"


class BigtablePredictionStorage:
    """Persist miner prediction paths as raw float32 blobs in Bigtable.

    Tables and instance are addressed via env vars (like the Postgres
    connection). The instance handle is shared across the two tables.
    """

    def __init__(self) -> None:
        project = _require_env(_ENV_PROJECT)
        instance_id = _require_env(_ENV_INSTANCE)
        self._table_low_id = _require_env(_ENV_TABLE_LOW)
        self._table_high_id = _require_env(_ENV_TABLE_HIGH)

        client = bigtable.Client(project=project, admin=False)
        instance = client.instance(instance_id)
        self._tables = {
            "low": instance.table(self._table_low_id),
            "high": instance.table(self._table_high_id),
        }

    @staticmethod
    def build_row_key(
        asset: str,
        prompt_label: str,
        start_time: str,
        miner_id: int,
    ) -> str:
        return f"{asset}#{prompt_label}#{start_time}#{miner_id}"

    def write_predictions(
        self,
        prompt_label: str,
        simulation_input: SimulationInput,
        miner_predictions: dict,
        miner_id_map: dict,
    ) -> dict:
        """Write CORRECT predictions for one request to Bigtable.

        Returns {miner_uid: bigtable_key} for rows that were written. Miners
        whose response failed format validation or whose miner_uid is not in
        miner_id_map are skipped.
        """
        table = self._table_for_label(prompt_label)

        rows = []
        keys_by_miner_uid: dict = {}
        for miner_uid, (
            prediction,
            format_validation,
            _process_time,
        ) in miner_predictions.items():
            if format_validation != response_validation_v2.CORRECT:
                continue
            if miner_uid not in miner_id_map:
                continue

            miner_id = miner_id_map[miner_uid]
            key = self.build_row_key(
                simulation_input.asset,
                prompt_label,
                simulation_input.start_time,
                miner_id,
            )
            blob = _paths_to_float32_bytes(prediction)

            row = table.direct_row(key)
            row.set_cell(COLUMN_FAMILY, COLUMN_QUALIFIER, blob)
            rows.append(row)
            keys_by_miner_uid[miner_uid] = key

        if not rows:
            return keys_by_miner_uid

        statuses = table.mutate_rows(rows)
        for key, status in zip(
            list(keys_by_miner_uid.values()), statuses, strict=False
        ):
            if status.code != 0:
                bt.logging.error(
                    f"bigtable write failed for key={key} "
                    f"code={status.code} message={status.message}"
                )

        return keys_by_miner_uid

    def read_predictions(
        self,
        items: list,
    ) -> dict:
        """Batch-read prediction blobs from Bigtable.

        `items` is a list of tuples `(bigtable_key, prompt_label,
        num_simulations, num_timesteps)`. Returns `{bigtable_key: paths}` where
        `paths` is `list[list[float]]` with shape (num_simulations,
        num_timesteps). Missing rows return `[]` (treated upstream as
        no-prediction).
        """
        grouped: dict = {}
        shape_by_key: dict = {}
        for key, prompt_label, num_simulations, num_timesteps in items:
            grouped.setdefault(prompt_label, []).append(key)
            shape_by_key[key] = (num_simulations, num_timesteps)

        result: dict = {key: [] for key, *_ in items}

        for prompt_label, keys in grouped.items():
            table = self._table_for_label(prompt_label)
            row_set = RowSet()
            for key in keys:
                row_set.add_row_key(key)
            row_filter = CellsColumnLimitFilter(1)
            for row in table.read_rows(row_set=row_set, filter_=row_filter):
                key = (
                    row.row_key.decode("utf-8")
                    if isinstance(row.row_key, bytes)
                    else row.row_key
                )
                cells = row.cells.get(COLUMN_FAMILY, {}).get(
                    COLUMN_QUALIFIER, []
                )
                if not cells:
                    continue
                num_simulations, num_timesteps = shape_by_key[key]
                result[key] = _float32_bytes_to_paths(
                    cells[0].value, num_simulations, num_timesteps
                )

        return result

    def _table_for_label(self, prompt_label: str):
        try:
            return self._tables[prompt_label]
        except KeyError as exc:
            raise ValueError(
                f"unsupported prompt_label for Bigtable: {prompt_label!r}"
            ) from exc


def _paths_to_float32_bytes(prediction) -> bytes:
    """Convert a validator-format prediction to raw float32 bytes.

    The on-the-wire prediction is `[start_ts, time_increment, path1, ...,
    pathN]` where each path is a list of floats. Only the paths are stored in
    Bigtable; the header is reconstructed from validator_requests metadata on
    read.
    """
    paths = prediction[2:]
    return np.asarray(paths, dtype=np.float32).tobytes()


def _float32_bytes_to_paths(
    blob: bytes, num_simulations: int, num_timesteps: int
) -> list:
    arr = np.frombuffer(blob, dtype=np.float32).reshape(
        num_simulations, num_timesteps
    )
    return arr.tolist()


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(
            f"environment variable {name} is required for Bigtable storage "
            f"backend"
        )
    return value
