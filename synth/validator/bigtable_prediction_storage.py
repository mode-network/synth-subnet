"""Bigtable storage backend for miner predictions.

Each (asset, start_time, miner) is one row, value = raw float32 bytes shaped
(num_simulations x num_timesteps). Two tables are used so retention is
enforced by per-table GC policy: one for the `low` prompt and one for the
`high` prompt. The table choice already encodes the prompt label, so the
row key omits it.

The Postgres `miner_predictions` row stays — its `prediction` column holds a
sentinel JSON, and `bigtable_key` holds the row key used here.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

import bittensor as bt
import numpy as np
from google.cloud import bigtable
from google.cloud.bigtable.row_filters import CellsColumnLimitFilter
from google.cloud.bigtable.row_set import RowRange, RowSet

from synth.simulation_input import SimulationInput
from synth.validator import prompt_config, response_validation_v2

COLUMN_FAMILY = "p"
COLUMN_QUALIFIER = b"d"

# Zero-pad miner_id so lexicographic order inside a range scan matches
# numeric order. 6 digits cover the foreseeable miners.id space (Postgres
# bigint surrogate; growing slowly).
_MINER_ID_PAD = 6

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
    def build_row_key(asset: str, start_time_unix: int, miner_id: int) -> str:
        """Compose the Bigtable row key.

        Leading `{asset}` (e.g. BTC, ETH, ...) interleaves writes across
        ~12 distinct prefixes, which is enough fan-out to keep sequential
        timestamps from hotspotting a single tablet. The trailing
        `{miner_id}` is zero-padded so lexicographic range scans match
        numeric order.

        `prompt_label` is intentionally NOT in the key — the table itself
        (low vs high) already encodes it.
        """
        return f"{asset}#{start_time_unix}#{miner_id:0{_MINER_ID_PAD}d}"

    def write_predictions(
        self,
        simulation_input: SimulationInput,
        miner_predictions: dict,
        miner_id_map: dict,
    ) -> dict:
        """Write CORRECT predictions for one request to Bigtable.

        Returns {miner_uid: bigtable_key} for rows that were successfully
        committed. Miners whose response failed format validation or whose
        miner_uid is not in miner_id_map are skipped. If any mutate fails,
        raises so the `@retry` on `save_responses` triggers — never report
        keys for rows whose write failed (would silently drop scoring data).
        """
        prompt_label = prompt_config.label_from_time_length(
            simulation_input.time_length
        )
        table = self._table_for_label(prompt_label)
        start_time_unix = _start_time_to_unix(simulation_input.start_time)

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
                bt.logging.warning(
                    f"bigtable write_predictions: miner_uid {miner_uid} "
                    f"not in miners table, skipping"
                )
                continue

            miner_id = miner_id_map[miner_uid]
            key = self.build_row_key(
                simulation_input.asset, start_time_unix, miner_id
            )
            blob = _paths_to_float32_bytes(prediction)

            row = table.direct_row(key)
            row.set_cell(COLUMN_FAMILY, COLUMN_QUALIFIER, blob)
            rows.append(row)
            keys_by_miner_uid[miner_uid] = key

        if not rows:
            return keys_by_miner_uid

        statuses = table.mutate_rows(rows)
        failed_keys = []
        for key, status in zip(
            list(keys_by_miner_uid.values()), statuses, strict=False
        ):
            if status.code != 0:
                failed_keys.append(key)
                bt.logging.error(
                    f"bigtable write failed for key={key} "
                    f"code={status.code} message={status.message}"
                )
        if failed_keys:
            # Surface the failure so save_responses' @retry triggers.
            # Letting the caller persist bigtable_key for a row whose
            # blob never landed would silently drop scoring data later.
            raise RuntimeError(
                f"bigtable mutate_rows failed for "
                f"{len(failed_keys)}/{len(rows)} rows"
            )

        return keys_by_miner_uid

    def read_predictions(
        self,
        validator_request,
        keys: list,
    ) -> dict:
        """Batch-read prediction blobs from Bigtable.

        `validator_request` carries the asset + start_time used to build the
        row-key prefix, the time_length used to pick the right table, and the
        shape used to reshape the float32 bytes. Returns `{bigtable_key:
        paths}` where `paths` is `list[list[float]]`. Missing or undecodable
        rows return `[]` (treated upstream as no-prediction).

        Uses a single range scan over `{asset}#{start_time_unix}#` — much
        cheaper than N point lookups at 256 miners per request.
        """
        result: dict = {key: [] for key in keys}
        if not keys:
            return result

        prompt_label = prompt_config.label_from_time_length(
            validator_request.time_length
        )
        num_simulations = int(validator_request.num_simulations)
        num_timesteps = (
            validator_request.time_length // validator_request.time_increment
            + 1
        )
        start_time_unix = int(validator_request.start_time.timestamp())
        prefix = f"{validator_request.asset}#{start_time_unix}#"

        # `~` (0x7e) sits above every digit (0x30-0x39), so this end-key
        # captures every row that starts with `prefix + <digits>`.
        row_range = RowRange(
            start_key=prefix.encode("utf-8"),
            end_key=(prefix + "~").encode("utf-8"),
        )
        row_set = RowSet()
        row_set.add_row_range(row_range)

        table = self._table_for_label(prompt_label)
        wanted = set(keys)
        for row in table.read_rows(
            row_set=row_set, filter_=CellsColumnLimitFilter(1)
        ):
            key = (
                row.row_key.decode("utf-8")
                if isinstance(row.row_key, bytes)
                else row.row_key
            )
            # The range scan also surfaces rows whose Postgres siblings
            # were soft-deleted (density tapering doesn't touch Bigtable).
            # Filter to the keys the caller asked for.
            if key not in wanted:
                continue
            cells = row.cells.get(COLUMN_FAMILY, {}).get(COLUMN_QUALIFIER, [])
            if not cells:
                continue
            try:
                result[key] = _float32_bytes_to_paths(
                    cells[0].value, num_simulations, num_timesteps
                )
            except Exception as e:
                # Corrupted / mis-shaped blob. Don't crash the whole
                # scoring loop on one bad row — leave it as [] so just
                # that miner is treated as no-prediction.
                bt.logging.warning(
                    f"bigtable read: failed to decode key={key}: {e}"
                )

        return result

    def _table_for_label(self, prompt_label: str):
        try:
            return self._tables[prompt_label]
        except KeyError as exc:
            raise ValueError(
                f"unsupported prompt_label for Bigtable: {prompt_label!r}"
            ) from exc


def _start_time_to_unix(start_time_str: str) -> int:
    """Convert the simulation_input.start_time ISO string to a unix int.

    Naive ISO strings are treated as UTC so the unix timestamp matches what
    Postgres-derived `validator_request.start_time` produces at read time.
    """
    dt = datetime.fromisoformat(start_time_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


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
