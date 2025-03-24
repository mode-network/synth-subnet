import os

import pandas as pd

from synth.validator.moving_average import compute_weighted_averages


def read_csv(file_name):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_dir, file_name)
    return pd.read_csv(file_path)


def test_moving_average_1():
    scored_time = "2025-02-21T17:23:00+00:00"
    half_life_days = 2

    df = read_csv("cutoff_data_4_days.csv")
    df["scored_time"] = pd.to_datetime(df["scored_time"])

    moving_averages_data = compute_weighted_averages(
        input_df=df,
        half_life_days=half_life_days,
        scored_time_str=scored_time,
        softmax_beta=-0.003,
    )

    # The miner id you want to search for
    target_id = 144

    # Select the element by miner id
    selected_miner = next(
        (
            item
            for item in moving_averages_data
            if item["miner_id"] == target_id
        ),
        None,
    )

    # Print the selected miner
    print("selected_miner", selected_miner)


def test_moving_average_2():
    scored_time = "2025-02-21T17:23:00+00:00"
    half_life_days = 1

    df = read_csv("cutoff_data_2_days.csv")
    df["scored_time"] = pd.to_datetime(df["scored_time"])

    moving_averages_data = compute_weighted_averages(
        input_df=df,
        half_life_days=half_life_days,
        scored_time_str=scored_time,
        softmax_beta=-0.003,
    )

    # The miner id you want to search for
    target_id = 144

    # Select the element by miner id
    selected_miner = next(
        (
            item
            for item in moving_averages_data
            if item["miner_id"] == target_id
        ),
        None,
    )

    print("selected_miner", selected_miner)


def test_get_miner_scores():
    scored_time = "2025-02-21T17:23:00+00:00"
    half_life_days = 1

    df = read_csv("cutoff_data_2_days.csv")
    df["scored_time"] = pd.to_datetime(df["scored_time"])

    moving_averages_data = compute_weighted_averages(
        input_df=df,
        half_life_days=half_life_days,
        scored_time_str=scored_time,
        softmax_beta=-0.003,
    )

    # The miner id you want to search for
    target_id = 144

    # Select the element by miner id
    selected_miner = next(
        (
            item
            for item in moving_averages_data
            if item["miner_id"] == target_id
        ),
        None,
    )

    # Print the selected miner
    print("selected_miner", selected_miner)
