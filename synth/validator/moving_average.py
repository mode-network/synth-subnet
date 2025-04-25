from datetime import datetime
import typing


import numpy as np
import pandas as pd
from pandas import DataFrame
import bittensor as bt


from synth.validator.miner_data_handler import MinerDataHandler
from synth.validator.reward import compute_softmax


def prepare_df_for_moving_average(df):
    """
    Prepare the input dataframe for the moving average computation.

    If a miner misses a prompt or has recently joined the network, we backfill
    the prompt_score_v2 and score_details_v2.

    To determine if a miner has missed a prompt, we check if the miner has a record
    at the global_min timestamp. If not, we assume the miner has missed the prompt.

    :param df: The input dataframe.
    :return: The prepared dataframe.
    """
    df["scored_time"] = pd.to_datetime(df["scored_time"])

    # Determine the global minimum scored_time and the complete (global) set of times.
    global_min = df["scored_time"].min()
    all_times = sorted(df["scored_time"].unique())

    # Create a global mapping for each scored_time to the corresponding worst prompt score.
    # Here we simply pick (for each timestamp) the percentile90 and the lowest_score from the first row encountered.
    global_worst_score_mapping = {}
    global_score_details_mapping = {}
    for t in all_times:
        sample_row = df.loc[df["scored_time"] == t].iloc[0]
        if sample_row["score_details_v2"] is None:
            continue
        global_worst_score_mapping[t] = (
            sample_row["score_details_v2"]["percentile90"]
            - sample_row["score_details_v2"]["lowest_score"]
        )
        global_score_details_mapping[t] = sample_row["score_details_v2"]

    def fill_missing_for_miner(group):
        miner_min = group["scored_time"].min()

        # We assume the miner is missing data if they did not start at the global_min.
        if miner_min > global_min:
            # Reindex using the full range of times
            new_index = pd.Index(all_times, name="scored_time")
            group = group.set_index("scored_time")
            group = group.reindex(new_index)

            # Fill in miner_id (assumed constant for the miner)
            group["miner_id"] = group["miner_id"].ffill().bfill().astype(int)

            # For missing prompt_score_v2, use the corresponding worst_score from the mapping.
            # Note: group.index is the scored_time.
            group["prompt_score_v2"] = group["prompt_score_v2"].fillna(
                group.index.to_series().map(global_worst_score_mapping)
            )

            # Fill in score_details_v2:
            group["score_details_v2"] = [
                global_score_details_mapping.get(t) for t in group.index
            ]

            group = group.reset_index()
        return group

    df = df.groupby("miner_id", group_keys=False).apply(fill_missing_for_miner)
    df = df.sort_values(by=["scored_time", "miner_id"])

    return df


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
            prompt_score = row["prompt_score_v2"]
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
    filtered_moving_averages_data = []
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
