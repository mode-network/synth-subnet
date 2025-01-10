from datetime import datetime, timezone

import pandas as pd
from pandas import DataFrame


def compute_weighted_averages(
    input_df: DataFrame,
    half_life_days: float,
    alpha: float,
    validation_time_str: str,
) -> []:
    """
    Reads a TSV file of miner rewards, computes an exponentially weighted
    moving average (EWMA) with a user-specified half-life, then outputs:
      1) The EWMA of each miner's reward
      2) EWMA^alpha, normalized across miners

    The file must have columns:
       - 'miner_uid'
       - 'reward'
       - 'scored_time'
    and be tab-separated.

    :param input_df: Dataframe of miner rewards.
    :param half_life_days: The half-life in days for the exponential decay.
    :param alpha: The exponent to raise the EWMA to, before normalization.
    :param validation_time_str: The current time when validator does the scoring.
    """
    if input_df.empty:
        return None

    validation_time = datetime.fromisoformat(validation_time_str).replace(
        tzinfo=timezone.utc
    )

    # Group by miner_uid
    grouped = input_df.groupby("miner_uid")

    results = []  # will hold tuples of (miner_uid, ewma)

    for miner_uid, group_df in grouped:
        total_weight = 0.0
        weighted_reward_sum = 0.0

        for _, row in group_df.iterrows():
            if pd.isna(row["prompt_score"]):
                continue  # skip missing or invalid reward

            w = compute_weight(
                row["scored_time"], validation_time, half_life_days
            )
            total_weight += w
            weighted_reward_sum += w * row["prompt_score"]

        ewma = (
            weighted_reward_sum / total_weight
            if total_weight > 0
            else float("nan")
        )
        results.append((miner_uid, ewma))

    # Now compute EWMA^alpha for each miner and normalize
    # If the EWMA is NaN, treat it as 0 for the power-sum.
    miner_uids = [r[0] for r in results]
    ewm_as = [r[1] for r in results]

    # Convert NaN to 0.0 for the exponent operation and sum
    ewm_as_nonan = [0.0 if pd.isna(x) else x for x in ewm_as]
    ewm_as_pow = [x**alpha for x in ewm_as_nonan]  # raise to alpha (default=2)

    pow_sum = sum(ewm_as_pow)

    # Avoid division by zero if all are zero
    if pow_sum <= 0:
        norm_scores = [0.0] * len(ewm_as_pow)
    else:
        norm_scores = [x / pow_sum for x in ewm_as_pow]

    rewards = []
    for (miner_uid, ewma_val), norm_val in zip(results, norm_scores):
        reward_item = {
            "miner_uid": miner_uid,
            "smoothed_score": float(ewma_val),
            "reward_weight": float(norm_val),
            "updated_at": validation_time_str,
        }
        rewards.append(reward_item)

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
