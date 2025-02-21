import os
from io import StringIO

import pandas as pd

from synth.validator.moving_average import compute_weighted_averages


def test_moving_average_1():
    scored_time = "2025-02-21T17:23:00+00:00"
    alpha = 4
    half_life_days = 2

    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_name = 'cutoff_data_4_days.csv'
    file_path = os.path.join(current_dir, file_name)

    df = pd.read_csv(file_path)
    df['scored_time'] = pd.to_datetime(df['scored_time'])

    moving_averages_data = compute_weighted_averages(
        input_df=df,
        half_life_days=half_life_days,
        alpha=alpha,
        validation_time_str=scored_time,
    )

    # The miner_uid you want to search for
    target_uid = 144

    # Select the element by miner_uid
    selected_miner = next((item for item in moving_averages_data if item['miner_uid'] == target_uid), None)

    # Print the selected miner
    print(selected_miner)


def test_moving_average_2():
    scored_time = "2025-02-21T17:23:00+00:00"
    alpha = 2
    half_life_days = 1

    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_name = 'cutoff_data_2_days.csv'
    file_path = os.path.join(current_dir, file_name)

    df = pd.read_csv(file_path)
    df['scored_time'] = pd.to_datetime(df['scored_time'])

    moving_averages_data = compute_weighted_averages(
        input_df=df,
        half_life_days=half_life_days,
        alpha=alpha,
        validation_time_str=scored_time,
    )

    # The miner_uid you want to search for
    target_uid = 144

    # Select the element by miner_uid
    selected_miner = next((item for item in moving_averages_data if item['miner_uid'] == target_uid), None)

    # Print the selected miner
    print(selected_miner)
