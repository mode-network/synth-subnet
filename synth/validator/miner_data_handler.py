from datetime import datetime, timedelta

import bittensor as bt
import pandas as pd
from sqlalchemy import insert, select, text
from sqlalchemy.types import String, TIMESTAMP, JSON
from sqlalchemy.sql import bindparam

from synth.db.models import (
    miner_predictions as miner_predictions_model,
    miners as miners_model,
    miner_scores,
    validator_requests,
    metagraph_history,
    miner_rewards,
    get_engine,
    weights_update_history,
)
from synth.simulation_input import SimulationInput
from synth.validator import response_validation


class MinerDataHandler:
    def __init__(self, engine=None):
        # Use the provided engine or fall back to the default engine
        self.engine = engine or get_engine()

    def save_responses(
        self,
        miner_predictions: dict,
        simulation_input: SimulationInput,
        request_time: datetime,
    ):
        """Save miner predictions and simulation input."""

        # Prepare the validator_requests row from the simulation input:
        validator_requests_row = {
            "start_time": simulation_input.start_time,
            "asset": simulation_input.asset,
            "time_increment": simulation_input.time_increment,
            "time_length": simulation_input.time_length,
            "num_simulations": simulation_input.num_simulations,
            "request_time": request_time.isoformat(),
        }

        try:
            with self.engine.connect() as connection:
                with connection.begin():
                    # 1. Insert into validator_requests and get its ID
                    insert_stmt_validator = insert(validator_requests).values(
                        validator_requests_row
                    )
                    result = connection.execute(insert_stmt_validator)
                    validator_requests_id = result.inserted_primary_key[0]

                    # 2. Build lists for each parameter from miner_predictions, which maps miner_uid -> (prediction, format_validation, process_time)
                    miner_uids = []
                    predictions = []
                    validations = []
                    process_times = []
                    for miner_uid, (
                        prediction,
                        format_validation,
                        process_time,
                    ) in miner_predictions.items():
                        miner_uids.append(miner_uid)
                        predictions.append(
                            prediction
                            if format_validation == response_validation.CORRECT
                            else []
                        )
                        validations.append(format_validation)
                        process_times.append(process_time)

                    # 3. Create a text-based subquery using unnest
                    # We use cast hints in the SQL to indicate the expected types.
                    # Note: Adjust the type casts (e.g., ::text[], ::json[], etc.) as needed
                    unnest_sql = text(
                        """
                        SELECT *
                        FROM unnest(
                            :miner_uid::text[],
                            :prediction::json[],
                            :validation::text[],
                            :process_time::timestamp[]
                        ) AS vals(miner_uid, prediction, format_validation, process_time)
                        """
                    ).bindparams(
                        bindparam("miner_uid", value=miner_uids),
                        bindparam("prediction", value=predictions),
                        bindparam("validation", value=validations),
                        bindparam("process_time", value=process_times),
                    )

                    # Tell SQLAlchemy about the columns of our text subquery.
                    values_subquery = unnest_sql.columns(
                        miner_uid=String,
                        prediction=JSON,
                        format_validation=String,
                        process_time=TIMESTAMP,
                    ).alias("vals")

                    # 4. Create an INSERT FROM SELECT statement.
                    # Here we join the miner table (to resolve miner.id by miner_uid)
                    insert_stmt_mp = insert(
                        miner_predictions_model
                    ).from_select(
                        [
                            "validator_requests_id",
                            "miner_id",  # will be looked up via join
                            "prediction",
                            "format_validation",
                            "process_time",
                        ],
                        select(
                            bindparam(
                                "validator_requests_id",
                                value=validator_requests_id,
                            ),
                            miners_model.c.id,
                            values_subquery.c.prediction,
                            values_subquery.c.format_validation,
                            values_subquery.c.process_time,
                        ).select_from(
                            miners_model.join(
                                values_subquery,
                                miners_model.c.miner_uid
                                == values_subquery.c.miner_uid,
                            )
                        ),
                    )

                    # 5. Execute the bulk insert statement
                    connection.execute(insert_stmt_mp)

            # Optionally, you can return validator_requests_id if needed.
            # return validator_requests_id

        except Exception as e:
            # With context-managed transactions the rollback happens automatically,
            # but we log the error for debugging.
            bt.logging.error(f"in save_responses (got an exception): {e}")

    def set_reward_details(self, reward_details: list[dict], scored_time: str):
        rows_to_insert = [
            {
                "miner_uid": row["miner_uid"],
                "scored_time": scored_time,
                "miner_predictions_id": row["miner_prediction_id"],
                "score_details_v2": {
                    "total_crps": row["total_crps"],
                    "percentile90": row["percentile90"],
                    "lowest_score": row["lowest_score"],
                    "prompt_score_v2": row["prompt_score_v2"],
                    "crps_data": row["crps_data"],
                },
                "prompt_score_v2": row["prompt_score_v2"],
                "real_prices": row["real_prices"],
            }
            for row in reward_details
        ]

        try:
            with self.engine.connect() as connection:
                with connection.begin():  # Begin a transaction
                    insert_stmt_miner_scores = miner_scores.insert().values(
                        rows_to_insert
                    )
                    connection.execute(insert_stmt_miner_scores)
        except Exception as e:
            connection.rollback()
            bt.logging.error(f"in set_reward_details (got an exception): {e}")

    def get_miner_prediction(self, miner_uid: int, validator_request_id: int):
        """Retrieve the record with the longest valid interval for the given miner_id."""
        try:
            with self.engine.connect() as connection:
                query = (
                    select(
                        miner_predictions_model.c.id,
                        miner_predictions_model.c.prediction,
                        miner_predictions_model.c.format_validation,
                    )
                    .select_from(miner_predictions_model)
                    .where(
                        miner_predictions_model.c.miner_uid == miner_uid,
                        miner_predictions_model.c.validator_requests_id
                        == validator_request_id,
                    )
                    .limit(1)
                )

                result = connection.execute(query).fetchone()

            if result is None:
                return None, [], ""

            record_id = result.id
            prediction = result.prediction
            format_validation = result.format_validation

            return record_id, prediction, format_validation
        except Exception as e:
            bt.logging.error(
                f"in get_miner_prediction (got an exception): {e}"
            )
            return None, [], ""

    def get_latest_prediction_request(
        self, scored_time_str: str, simulation_input: SimulationInput
    ):
        """Retrieve the id of the latest validator request that (start_time + time_length) < scored_time."""
        try:
            scored_time = datetime.fromisoformat(scored_time_str)

            with self.engine.connect() as connection:
                query = (
                    select(validator_requests.c.id)
                    .select_from(validator_requests)
                    .where(
                        (
                            validator_requests.c.start_time
                            + text("INTERVAL '1 second'")
                            * validator_requests.c.time_length
                        )
                        < scored_time,
                        validator_requests.c.asset == simulation_input.asset,
                        validator_requests.c.time_increment
                        == simulation_input.time_increment,
                        validator_requests.c.time_length
                        == simulation_input.time_length,
                        validator_requests.c.num_simulations
                        == simulation_input.num_simulations,
                    )
                    .order_by(validator_requests.c.start_time.desc())
                    .limit(1)
                )

                result = connection.execute(query).fetchone()

                if result is None:
                    return None

                return result.id
        except Exception as e:
            bt.logging.error(
                f"in get_latest_prediction_request (got an exception): {e}"
            )
            return None

    def update_metagraph_history(self, metagraph_info: []):
        try:
            with self.engine.connect() as connection:
                with connection.begin():  # Begin a transaction
                    insert_stmt = metagraph_history.insert().values(
                        metagraph_info
                    )
                    connection.execute(insert_stmt)
        except Exception as e:
            if connection:
                connection.rollback()
            bt.logging.error(
                f"in update_metagraph_history (got an exception): {e}"
            )

    def get_miner_scores(self, scored_time_str: str, cutoff_days: int):
        scored_time = datetime.fromisoformat(scored_time_str)
        min_scored_time = scored_time - timedelta(days=cutoff_days)

        try:
            with self.engine.connect() as connection:
                query = (
                    select(
                        miner_scores.c.miner_uid,
                        miner_scores.c.prompt_score_v2,
                        miner_scores.c.scored_time,
                        miner_scores.c.score_details_v2,
                    )
                    .select_from(miner_scores)
                    .where(miner_scores.c.scored_time > min_scored_time)
                )

                result = connection.execute(query)

            df = pd.DataFrame(result.fetchall(), columns=result.keys())

            return df
        except Exception as e:
            bt.logging.error(f"in get_miner_scores (got an exception): {e}")
            return pd.DataFrame()

    def update_miner_rewards(self, miner_rewards_data: list[dict]):
        try:
            with self.engine.connect() as connection:
                with connection.begin():  # Begin a transaction
                    insert_stmt = miner_rewards.insert().values(
                        miner_rewards_data
                    )
                    connection.execute(insert_stmt)
        except Exception as e:
            connection.rollback()
            bt.logging.error(
                f"in update_miner_rewards (got an exception): {e}"
            )

    def update_weights_history(
        self,
        miner_uids: list[int],
        miner_weights: list[float],
        norm_miner_uids: list[int],
        norm_miner_weights: list[int],
        update_result: str,
        scored_time: str,
    ):
        update_weights_rows = {
            "miner_uids": miner_uids,
            "miner_weights": miner_weights,
            "norm_miner_uids": norm_miner_uids,
            "norm_miner_weights": norm_miner_weights,
            "update_result": update_result,
            "updated_at": scored_time,
        }

        try:
            with self.engine.connect() as connection:
                with connection.begin():  # Begin a transaction
                    insert_stmt = weights_update_history.insert().values(
                        update_weights_rows
                    )
                    connection.execute(insert_stmt)
        except Exception as e:
            connection.rollback()
            bt.logging.error(
                f"in update_weights_history (got an exception): {e}"
            )
