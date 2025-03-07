from datetime import datetime, timezone

import numpy as np
from pandas import DataFrame

from synth.validator.reward import compute_softmax


def compute_weighted_averages(
    input_df: DataFrame,
    half_life_days: float,
    scored_time_str: str,
    softmax_beta: float,
) -> list[dict]:
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

    validation_time = datetime.fromisoformat(scored_time_str).replace(
        tzinfo=timezone.utc
    )

    # Group by miner_uid
    grouped = input_df.groupby("miner_uid")

    results = []  # will hold tuples of (miner_uid, ewma)

    for miner_uid, group_df in grouped:
        total_weight = 0.0
        weighted_reward_sum = 0.0

        for _, row in group_df.iterrows():
            prompt_score = row["prompt_score_v2"]
            if prompt_score is None or np.isnan(prompt_score):
                continue

            w = compute_weight(
                row["scored_time"], validation_time, half_life_days
            )
            total_weight += w
            weighted_reward_sum += w * prompt_score

        ewma = (
            weighted_reward_sum / total_weight
            if total_weight > 0
            else float("inf")
        )
        results.append((miner_uid, ewma))

    # Now compute soft max to get the reward_scores
    ewma_list = [r[1] for r in results]
    reward_weight_list = compute_softmax(np.array(ewma_list), softmax_beta)

    rewards = []
    for (miner_uid, ewma), reward_weight in zip(results, reward_weight_list):
        reward_item = {
            "miner_uid": miner_uid,
            "smoothed_score": float(ewma),
            "reward_weight": float(reward_weight),
            "updated_at": scored_time_str,
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
