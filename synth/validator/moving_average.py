from datetime import datetime
import typing


import numpy as np
import pandas as pd
from pandas import DataFrame
import bittensor as bt


from synth.validator.miner_data_handler import MinerDataHandler
from synth.validator.reward import compute_softmax


def prepare_df_for_moving_average(df):
    df = df.copy()
    df["scored_time"] = pd.to_datetime(df["scored_time"])

    # 0) Temporary exclude a period
    df["start_time"] = pd.to_datetime(df["start_time"])
    exclude_start = datetime.fromisoformat("2025-11-18 11:53:00+00:00")
    exclude_end = datetime.fromisoformat("2025-11-18 14:08:00+00:00")
    mask_exclude = (df["start_time"] >= exclude_start) & (
        df["start_time"] <= exclude_end
    )
    df = df.loc[~mask_exclude]

    # 1) compute globals
    global_min = df["scored_time"].min()
    all_times = sorted(df["scored_time"].unique())

    # build your global‐worst‐score mappings exactly as you had them
    global_worst_score_mapping = {}
    global_score_details_mapping = {}
    global_score_asset_mapping = {}
    for t in all_times:
        sample = df.loc[df["scored_time"] == t].iloc[0]
        details = sample["score_details_v3"]
        if details is None:
            continue
        global_worst_score_mapping[t] = (
            details["percentile90"] - details["lowest_score"]
        )
        global_score_details_mapping[t] = details
        global_score_asset_mapping[t] = sample["asset"]

    # 2) find, for each miner, when they first appear
    miner_first = (
        df.groupby("miner_id")["scored_time"]
        .min()
        .rename("miner_min")
        .reset_index()
    )

    # 3) build the full cartesian product of miner_id × all_times
    miners = df[["miner_id"]].drop_duplicates()
    full = (
        miners.assign(_tmp=1)
        .merge(pd.DataFrame({"scored_time": all_times, "_tmp": 1}), on="_tmp")
        .drop(columns="_tmp")
    )

    # 4) left‐merge the real data onto that grid
    full = full.merge(df, on=["miner_id", "scored_time"], how="left").merge(
        miner_first, on="miner_id", how="left"
    )

    # 5) now vectorize the “new‐miner” backfill logic:
    is_new = full["miner_min"] > global_min

    # backfill prompt_score_v3 for new miners
    full.loc[is_new, "prompt_score_v3"] = full.loc[
        is_new, "prompt_score_v3"
    ].fillna(full.loc[is_new, "scored_time"].map(global_worst_score_mapping))

    # overwrite score_details_v3 for new miners
    full.loc[is_new, "score_details_v3"] = full.loc[is_new, "scored_time"].map(
        global_score_details_mapping
    )

    # overwrite asset for new miners
    full.loc[is_new, "asset"] = full.loc[is_new, "scored_time"].map(
        global_score_asset_mapping
    )

    # 6) drop the “fake” rows we only introduced for existing miners
    is_old = full["miner_min"] == global_min
    was_missing = (
        full["prompt_score_v3"].isna() & full["score_details_v3"].isna()
    )
    mask_drop = is_old & was_missing
    out = full.loc[
        ~mask_drop,
        [
            "scored_time",
            "miner_id",
            "prompt_score_v3",
            "score_details_v3",
            "asset",
        ],
    ]

    # 7) clean up types & sort
    out["miner_id"] = out["miner_id"].astype(int)
    out = out.sort_values(["scored_time", "miner_id"]).reset_index(drop=True)
    return out


def apply_per_asset_coefficients(
    df: DataFrame,
) -> DataFrame:
    # Define coefficients for each asset
    asset_coefficients = {
        "BTC": 1.0,
        "ETH": 0.6210893136676585,
        "XAU": 1.4550630831254674,
        "SOL": 0.5021491038021751,
    }

    sum_coefficients = 0.0

    for asset, coef in asset_coefficients.items():
        df.loc[df["asset"] == asset, "prompt_score_v3"] *= coef
        sum_coefficients += coef * len(df.loc[df["asset"] == asset])

    df["prompt_score_v3"] /= sum_coefficients

    return df["prompt_score_v3"]


def compute_smoothed_score(
    miner_data_handler: MinerDataHandler,
    input_df: DataFrame,
    window_days: int,
    scored_time: datetime,
    softmax_beta: float,
) -> typing.Optional[list[dict]]:
    if input_df.empty:
        return None

    # Group by miner_id
    grouped = input_df.groupby("miner_id")

    rolling_avg_data = []  # will hold dict with miner_id and rolling average

    for miner_id, group_df in grouped:
        # Ensure scored_time is datetime and sort
        group_df = group_df.copy()
        group_df["scored_time"] = pd.to_datetime(group_df["scored_time"])
        group_df = group_df.sort_values("scored_time")

        # Only consider rows within the last 10 days from scored_time
        min_time = scored_time - pd.Timedelta(days=window_days)
        mask = (group_df["scored_time"] > min_time) & (
            group_df["scored_time"] <= scored_time
        )
        window_df = group_df.loc[mask]

        # Drop NaN prompt_score_v3
        valid_scores = window_df[["prompt_score_v3", "asset"]].dropna()

        # Apply per-asset coefficients
        window_df = apply_per_asset_coefficients(valid_scores)

        if not window_df.empty:
            rolling_avg = float(window_df.sum())
        else:
            bt.logging.warning(
                f"Miner ID {miner_id} has no valid scores in the window. Assigning infinite rolling average."
            )
            rolling_avg = float("inf")

        rolling_avg_data.append(
            {"miner_id": miner_id, "rolling_avg": rolling_avg}
        )

    # Add the miner UID to the results
    moving_averages_data = miner_data_handler.populate_miner_uid_in_miner_data(
        rolling_avg_data
    )

    # Filter out None UID
    filtered_moving_averages_data: list[dict] = []
    for item in moving_averages_data:
        if item["miner_uid"] is not None:
            filtered_moving_averages_data.append(item)

    # Now compute soft max to get the reward_scores
    rolling_avg_list = [
        r["rolling_avg"] for r in filtered_moving_averages_data
    ]
    reward_weight_list = compute_softmax(
        np.array(rolling_avg_list), softmax_beta
    )

    rewards = []
    for item, reward_weight in zip(
        filtered_moving_averages_data, reward_weight_list
    ):
        # filter out zero rewards
        if float(reward_weight) > 0:
            rewards.append(
                {
                    "miner_id": item["miner_id"],
                    "miner_uid": item["miner_uid"],
                    "smoothed_score": item["rolling_avg"],
                    "reward_weight": float(reward_weight),
                    "updated_at": scored_time.isoformat(),
                }
            )

    return rewards


def print_rewards_df(moving_averages_data):
    bt.logging.info("Scored responses moving averages:")
    df = pd.DataFrame.from_dict(moving_averages_data)
    bt.logging.info(df.to_string())
