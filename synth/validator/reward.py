# The MIT License (MIT)
# Copyright © 2023 Yuma Rao
# TODO(developer): Set your name
# Copyright © 2023 <your name>

# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
# documentation files (the “Software”), to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
# and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all copies or substantial portions of
# the Software.

import typing
import traceback
import sys

# THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
# THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.
import numpy as np
import pandas as pd
import bittensor as bt


from synth.db.models import ValidatorRequest
from synth.utils.helpers import adjust_predictions
from synth.validator.crps_calculation import calculate_crps_for_miner
from synth.validator.miner_data_handler import MinerDataHandler
from synth.validator.price_data_provider import PriceDataProvider
from synth.validator import response_validation_v2


def reward(
    miner_data_handler: MinerDataHandler,
    miner_uid: int,
    time_increment: int,
    validator_request_id: int,
    real_prices: list[float],
):
    """
    Reward the miner response to the simulation_input request. This method returns a reward
    value for the miner, which is used to update the miner's score.

    Returns:
    - float: The reward value for the miner.
    """

    miner_prediction = miner_data_handler.get_miner_prediction(
        miner_uid, validator_request_id
    )

    if miner_prediction is None:
        return -1, [], None

    if miner_prediction.format_validation != response_validation_v2.CORRECT:
        # represents no prediction data from the miner
        return -1, [], miner_prediction

    if len(real_prices) == 0:
        return -1, [], miner_prediction

    predictions_path = adjust_predictions(miner_prediction.prediction)
    simulation_runs = np.array(predictions_path).astype(float)

    try:
        score, detailed_crps_data = calculate_crps_for_miner(
            simulation_runs,
            np.array(real_prices),
            time_increment,
        )
    except Exception as e:
        bt.logging.error(
            f"Error calculating CRPS for miner {miner_uid} with prediction_id {miner_prediction.id}: {e}"
        )
        traceback.print_exc(file=sys.stderr)
        return -1, [], miner_prediction

    # if score is nan, return -1
    if np.isnan(score):
        bt.logger.warning(
            f"CRPS calculation returned NaN for miner {miner_uid} with prediction_id {miner_prediction.id}"
        )
        return -1, detailed_crps_data, miner_prediction

    return score, detailed_crps_data, miner_prediction


def get_rewards(
    miner_data_handler: MinerDataHandler,
    price_data_provider: PriceDataProvider,
    validator_request: ValidatorRequest,
) -> tuple[typing.Optional[np.ndarray], list, list[dict]]:
    """
    Returns an array of rewards for the given query and responses.

    Args:
    - query (int): The query sent to the miner.
    - responses (List[float]): A list of responses from the miner.

    Returns:
    - np.ndarray: An array of rewards for the given query and responses.
    """

    miner_uids = miner_data_handler.get_miner_uid_of_prediction_request(
        validator_request.id
    )

    if miner_uids is None:
        return None, [], []

    try:
        real_prices = price_data_provider.fetch_data(validator_request)
    except Exception as e:
        bt.logging.warning(
            f"Error fetching data for validator request {validator_request.id}: {e}"
        )
        return None, [], []

    scores = []
    detailed_crps_data_list = []
    miner_prediction_list = []
    for miner_uid in miner_uids:
        # function that calculates a score for an individual miner
        score, detailed_crps_data, miner_prediction = reward(
            miner_data_handler,
            miner_uid,
            validator_request.time_increment,
            validator_request.id,
            real_prices,
        )
        scores.append(score)
        detailed_crps_data_list.append(detailed_crps_data)
        miner_prediction_list.append(miner_prediction)

    score_values = np.array(scores)
    prompt_scores, percentile90, lowest_score = compute_prompt_scores(
        score_values
    )

    if prompt_scores is None:
        return None, [], []

    # gather all the detailed information
    # for log and debug purposes
    detailed_info = [
        {
            "miner_uid": miner_uid,
            "prompt_score_v3": float(prompt_score),
            "percentile90": float(percentile90),
            "lowest_score": float(lowest_score),
            "miner_prediction_id": (
                miner_prediction.id if miner_prediction else None
            ),
            "format_validation": (
                miner_prediction.format_validation
                if miner_prediction
                else None
            ),
            "process_time": (
                miner_prediction.process_time if miner_prediction else None
            ),
            "total_crps": float(score),
            "crps_data": clean_numpy_in_crps_data(crps_data),
        }
        for miner_uid, score, crps_data, prompt_score, miner_prediction in zip(
            miner_uids,
            scores,
            detailed_crps_data_list,
            prompt_scores,
            miner_prediction_list,
        )
    ]

    return prompt_scores, detailed_info, real_prices


def compute_prompt_scores(score_values: np.ndarray):
    if np.all(score_values == -1):
        return None, 0, 0
    score_values_valid = score_values[score_values != -1]
    percentile90 = np.percentile(score_values_valid, 90)
    capped_scores = np.minimum(score_values, percentile90)
    capped_scores = np.where(score_values == -1, percentile90, capped_scores)
    lowest_score = np.min(capped_scores)
    return capped_scores - lowest_score, percentile90, lowest_score


def compute_softmax(score_values: np.ndarray, beta: float) -> np.ndarray:
    bt.logging.info(f"Going to use the following value of beta: {beta}")

    exp_scores = np.exp(beta * score_values)
    softmax_scores_valid = exp_scores / np.sum(exp_scores)
    softmax_scores = softmax_scores_valid

    return softmax_scores


def clean_numpy_in_crps_data(crps_data: list) -> list:
    cleaned_crps_data = [
        {
            key: (float(value) if isinstance(value, np.float64) else value)
            for key, value in item.items()
        }
        for item in crps_data
    ]
    return cleaned_crps_data


def print_scores_df(prompt_scores, detailed_info):
    bt.logging.info(f"Scored responses: {prompt_scores}")

    df = pd.DataFrame.from_dict(detailed_info)
    if df.empty:
        bt.logging.info("No data to display.")
        return
    # Drop columns that are not needed for logging
    if "crps_data" in df.columns:
        df = df.drop(columns=["crps_data"])
    if "real_prices" in df.columns:
        df = df.drop(columns=["real_prices"])
    bt.logging.info(df.to_string())
