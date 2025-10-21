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

    # 1) compute globals
    global_min = df["scored_time"].min()
    all_times = sorted(df["scored_time"].unique())

    # build your global‐worst‐score mappings exactly as you had them
    global_worst_score_mapping = {}
    global_score_details_mapping = {}
    for t in all_times:
        sample = df.loc[df["scored_time"] == t].iloc[0]
        details = sample["score_details_v3"]
        if details is None:
            continue
        global_worst_score_mapping[t] = (
            details["percentile90"] - details["lowest_score"]
        )
        global_score_details_mapping[t] = details

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

    # 6) drop the “fake” rows we only introduced for existing miners
    is_old = full["miner_min"] == global_min
    was_missing = (
        full["prompt_score_v3"].isna() & full["score_details_v3"].isna()
    )
    mask_drop = is_old & was_missing
    out = full.loc[
        ~mask_drop,
        ["scored_time", "miner_id", "prompt_score_v3", "score_details_v3"],
    ]

    # 7) clean up types & sort
    out["miner_id"] = out["miner_id"].astype(int)
    out = out.sort_values(["scored_time", "miner_id"]).reset_index(drop=True)
    return out


def compute_weighted_averages(
    miner_data_handler: MinerDataHandler,
    input_df: DataFrame,
    half_life_days: float,
    scored_time: datetime,
    softmax_beta: float,
) -> typing.Optional[list[dict]]:
    """
    Computes an exponentially weighted
    moving average (EWMA) with a user-specified half-life, then outputs:
      1) The EWMA of each miner's reward
      2) Softmax of the EWMA to get the reward scores

    :param input_df: Dataframe of miner rewards.
    :param half_life_days: The half-life in days for the exponential decay.
    :param scored_time_str: The current time when validator does the scoring.
    """
    if input_df.empty:
        return None

    # Group by miner_id
    grouped = input_df.groupby("miner_id")

    results = []  # will hold dict with miner_id and ewma

    for miner_id, group_df in grouped:
        total_weight = 0.0
        weighted_reward_sum = 0.0

        for _, row in group_df.iterrows():
            prompt_score = row["prompt_score_v3"]
            if prompt_score is None or np.isnan(prompt_score):
                continue

            w = compute_weight(row["scored_time"], scored_time, half_life_days)
            total_weight += w
            weighted_reward_sum += w * prompt_score

        ewma = (
            weighted_reward_sum / total_weight
            if total_weight > 0
            else float("inf")
        )
        results.append({"miner_id": miner_id, "ewma": ewma})

    # Add the miner UID to the results
    moving_averages_data = miner_data_handler.populate_miner_uid_in_miner_data(
        results
    )

    # Filter out None UID
    filtered_moving_averages_data: list[dict] = []
    for item in moving_averages_data:
        if item["miner_uid"] is not None:
            filtered_moving_averages_data.append(item)

    # Now compute soft max to get the reward_scores
    ewma_list = [r["ewma"] for r in filtered_moving_averages_data]
    reward_weight_list = compute_softmax(np.array(ewma_list), softmax_beta)

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
                    "smoothed_score": item["ewma"],
                    "reward_weight": float(reward_weight),
                    "updated_at": scored_time.isoformat(),
                }
            )

    return rewards


def compute_weight(
    scored_dt: datetime, validation_time: datetime, half_life_days: float
) -> float:
    """
    For a row with timestamp scored_dt, the age in days is delta_days.
    weight = 0.5^(delta_days / half_life_days), meaning that
    after 'half_life_days' days, the weight decays to 0.5.
    """
    delta_days = (validation_time - scored_dt).total_seconds() / (
        24.0 * 3600.0
    )
    return 0.5 ** (delta_days / half_life_days)


def print_rewards_df(moving_averages_data):
    bt.logging.info("Scored responses moving averages:")
    df = pd.DataFrame.from_dict(moving_averages_data)
    bt.logging.info(df.to_string())
