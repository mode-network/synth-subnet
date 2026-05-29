STORAGE_BACKEND_POSTGRES = "postgres"
STORAGE_BACKEND_BIGTABLE = "bigtable"

STORAGE_BACKEND_CHOICES = (
    STORAGE_BACKEND_POSTGRES,
    STORAGE_BACKEND_BIGTABLE,
)

# Sentinel stored in `miner_predictions.prediction` when the actual prediction
# payload lives in Bigtable. The row key is in `miner_predictions.bigtable_key`.
BIGTABLE_SENTINEL = {"stored": STORAGE_BACKEND_BIGTABLE}

# Synthetic `format_validation` value assigned at read time when a row's
# Bigtable blob can't be hydrated (row missing / undecodable). The scoring
# worker (`reward.py:_crps_worker`) short-circuits on any non-CORRECT
# value, so this flips an infra-side failure away from looking like a
# miner-side CRPS error.
BIGTABLE_MISSING_FORMAT = "bigtable: row missing"
