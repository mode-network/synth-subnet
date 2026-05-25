STORAGE_BACKEND_POSTGRES = "postgres"
STORAGE_BACKEND_BIGTABLE = "bigtable"

STORAGE_BACKEND_CHOICES = (
    STORAGE_BACKEND_POSTGRES,
    STORAGE_BACKEND_BIGTABLE,
)

# Sentinel stored in `miner_predictions.prediction` when the actual prediction
# payload lives in Bigtable. The row key is in `miner_predictions.bigtable_key`.
BIGTABLE_SENTINEL = {"stored": STORAGE_BACKEND_BIGTABLE}
