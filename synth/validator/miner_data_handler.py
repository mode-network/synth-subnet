from datetime import datetime, timedelta
import traceback
import sys
import typing


import bittensor as bt
import pandas as pd
from sqlalchemy import (
    Connection,
    Engine,
    and_,
    exists,
    select,
    text,
    func,
    desc,
    not_,
)
from sqlalchemy.dialects.postgresql import insert


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
    def __init__(self, engine: typing.Optional[Engine] = None):
        # Use the provided engine or fall back to the default engine
        self.engine = engine or get_engine()

    def get_miner_uids(self, connection: Connection):
        ranked_miners = select(
            miners_model,
            func.row_number()
            .over(
                partition_by=miners_model.c.miner_uid,
                order_by=desc(miners_model.c.updated_at),
            )
            .label("rn"),
        ).alias("ranked_miners")
        query = select(ranked_miners.c.id, ranked_miners.c.miner_uid).where(
            ranked_miners.c.rn == 1
        )
        return connection.execute(query)

    def get_miner_uids_map(self, connection: Connection):
        miners = self.get_miner_uids(connection)

        # map miner_uid -> miner_id
        miner_id_map = {}
        for row in miners:
            miner_id_map[row.miner_uid] = row.id

        return miner_id_map

    def get_miner_ids_map(self, connection: Connection):
        miners = self.get_miner_uids(connection)

        # map miner_id -> miner_uid
        miner_Uid_map = {}
        for row in miners:
            miner_Uid_map[row.id] = row.miner_uid

        return miner_Uid_map

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
                    # Insert into validator_requests and get its ID
                    insert_stmt_validator = insert(validator_requests).values(
                        validator_requests_row
                    )
                    result = connection.execute(insert_stmt_validator)
                    validator_requests_id = result.inserted_primary_key[0]

                    # Create the records to insert
                    miner_id_map = self.get_miner_uids_map(connection)
                    miner_prediction_records = []

                    for miner_uid, (
                        prediction,
                        format_validation,
                        process_time,
                    ) in miner_predictions.items():
                        if miner_uid not in miner_id_map:
                            bt.logging.error(
                                f"in save_responses, miner_uid {miner_uid} not found in miners table"
                            )
                            continue
                        miner_id = miner_id_map[miner_uid]
                        miner_prediction_records.append(
                            {
                                "validator_requests_id": validator_requests_id,
                                "miner_uid": miner_uid,  # deprecated
                                "miner_id": miner_id,
                                "prediction": (
                                    prediction
                                    if format_validation
                                    == response_validation.CORRECT
                                    else []
                                ),
                                "format_validation": format_validation,
                                "process_time": process_time,
                            }
                        )

                    # 4. Insert into miners table
                    if len(miner_prediction_records) == 0:
                        return None
                    insert_stmt_miner_predictions = insert(
                        miner_predictions_model
                    ).values(miner_prediction_records)
                    connection.execute(insert_stmt_miner_predictions)
            return validator_requests_id  # TODO: finish this: refactor to add the validator_requests_id in the score and reward table
        except Exception as e:
            bt.logging.error(f"in save_responses (got an exception): {e}")
            traceback.print_exc(file=sys.stderr)

    def set_miner_scores(self, reward_details: list[dict], scored_time: str):
        try:
            with self.engine.connect() as connection:
                with connection.begin():
                    rows_to_insert = []
                    for row in reward_details:
                        rows_to_insert.append(
                            {
                                "miner_uid": row["miner_uid"],  # deprecated
                                "scored_time": scored_time,
                                "miner_predictions_id": row[
                                    "miner_prediction_id"
                                ],
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
                        )

                    insert_stmt_miner_scores = insert(miner_scores).values(
                        rows_to_insert
                    )
                    connection.execute(insert_stmt_miner_scores)
        except Exception as e:
            bt.logging.error(f"in set_miner_scores (got an exception): {e}")
            traceback.print_exc(file=sys.stderr)

    def get_miner_uid_of_prediction_request(
        self, validator_request_id: int
    ) -> typing.Optional[list[int]]:
        """Retrieve the miner_uid of the given validator_request_id."""
        try:
            with self.engine.connect() as connection:
                query = (
                    select(
                        miners_model.c.miner_uid,
                    )
                    .select_from(miner_predictions_model)
                    .join(
                        miners_model,
                        miners_model.c.id
                        == miner_predictions_model.c.miner_id,
                    )
                    .where(
                        miner_predictions_model.c.validator_requests_id
                        == validator_request_id
                    )
                )

                data = connection.execute(query).fetchall()
                result = []
                for row in data:
                    result.append(row.miner_uid)

            return result
        except Exception as e:
            bt.logging.error(
                f"in get_miner_uid_of_prediction_request (got an exception): {e}"
            )
            traceback.print_exc(file=sys.stderr)
            return None

    def get_miner_prediction(self, miner_uid: int, validator_request_id: int):
        """Retrieve the record with the longest valid interval for the given miner_id."""
        try:
            with self.engine.connect() as connection:
                query = (
                    select(
                        miner_predictions_model.c.id,
                        miner_predictions_model.c.prediction,
                        miner_predictions_model.c.format_validation,
                        miner_predictions_model.c.process_time,
                    )
                    .select_from(miner_predictions_model)
                    .join(
                        miners_model,
                        miners_model.c.id
                        == miner_predictions_model.c.miner_id,
                    )
                    .where(
                        miners_model.c.miner_uid == miner_uid,
                        miner_predictions_model.c.validator_requests_id
                        == validator_request_id,
                    )
                    .limit(1)
                )

                result = connection.execute(query).fetchone()

            return result
        except Exception as e:
            bt.logging.error(
                f"in get_miner_prediction (got an exception): {e}"
            )
            traceback.print_exc(file=sys.stderr)
            return None

    def get_latest_prediction_requests(
        self, scored_time_str: str, simulation_input: SimulationInput
    ):
        """Retrieve the list of IDs of the latest validator requests that (start_time + time_length) < scored_time."""
        try:
            scored_time = datetime.fromisoformat(scored_time_str)

            with self.engine.connect() as connection:
                subq = (
                    select(1)
                    .select_from(
                        miner_scores.join(
                            miner_predictions_model,
                            miner_predictions_model.c.id
                            == miner_scores.c.miner_predictions_id,
                        )
                    )
                    .where(
                        miner_predictions_model.c.validator_requests_id
                        == validator_requests.c.id
                    )
                )

                query = (
                    select(
                        validator_requests.c.id,
                        validator_requests.c.start_time,
                        validator_requests.c.time_length,
                        validator_requests.c.time_increment,
                    )
                    .where(
                        and_(
                            # Compare start_time plus an interval (in seconds) to the scored_time.
                            (
                                validator_requests.c.start_time
                                + text("INTERVAL '1 second'")
                                * validator_requests.c.time_length
                            )
                            < scored_time,
                            # Include simulation_input filters.
                            validator_requests.c.asset
                            == simulation_input.asset,
                            validator_requests.c.time_increment
                            == simulation_input.time_increment,
                            validator_requests.c.time_length
                            == simulation_input.time_length,
                            validator_requests.c.num_simulations
                            == simulation_input.num_simulations,
                            # Exclude records that have a matching miner_prediction via the NOT EXISTS clause.
                            not_(exists(subq)),
                        )
                    )
                    .order_by(validator_requests.c.start_time.desc())
                )

                return connection.execute(query).fetchall()
        except Exception as e:
            bt.logging.error(
                f"in get_latest_prediction_request (got an exception): {e}"
            )
            traceback.print_exc(file=sys.stderr)
            return None

    def insert_new_miners(self, metagraph_info: list):
        """Insert or update miners table with the provided data."""
        try:
            with self.engine.connect() as connection:
                with connection.begin():
                    insert_stmt = (
                        insert(miners_model)
                        .values(
                            [
                                {
                                    "miner_uid": miner["neuron_uid"],
                                    "coldkey": miner["coldkey"],
                                    "hotkey": miner["hotkey"],
                                }
                                for miner in metagraph_info
                            ]
                        )
                        .on_conflict_do_update(
                            # index_elements=["miner_uid", "coldkey", "hotkey"],
                            constraint="uq_miners_miner_uid_coldkey_hotkey",
                            # update the updated_at column
                            set_={"updated_at": datetime.now()},
                        )
                    )
                    connection.execute(insert_stmt)
        except Exception as e:
            bt.logging.error(
                f"in insert_or_update_miners (got an exception): {e}"
            )
            traceback.print_exc(file=sys.stderr)

    def update_metagraph_history(self, metagraph_info: list):
        try:
            with self.engine.connect() as connection:
                with connection.begin():
                    insert_stmt = insert(metagraph_history).values(
                        metagraph_info
                    )
                    connection.execute(insert_stmt)
        except Exception as e:
            bt.logging.error(
                f"in update_metagraph_history (got an exception): {e}"
            )
            traceback.print_exc(file=sys.stderr)

    def get_miner_scores(self, scored_time_str: str, cutoff_days: int):
        scored_time = datetime.fromisoformat(scored_time_str)
        min_scored_time = scored_time - timedelta(days=cutoff_days)

        try:
            with self.engine.connect() as connection:
                query = (
                    select(
                        miner_predictions_model.c.miner_id,
                        miner_scores.c.prompt_score_v2,
                        miner_scores.c.scored_time,
                        miner_scores.c.score_details_v2,
                    )
                    .select_from(miner_scores)
                    .join(
                        miner_predictions_model,
                        miner_predictions_model.c.id
                        == miner_scores.c.miner_predictions_id,
                    )
                    .where(miner_scores.c.scored_time > min_scored_time)
                )

                result = connection.execute(query)

            return pd.DataFrame(result.fetchall(), columns=list(result.keys()))
        except Exception as e:
            bt.logging.error(f"in get_miner_scores (got an exception): {e}")
            traceback.print_exc(file=sys.stderr)
            return pd.DataFrame()

    def populate_miner_uid_in_miner_data(self, miner_data: list[dict]):
        try:
            with self.engine.connect() as connection:
                with connection.begin():
                    miner_uid_map = self.get_miner_ids_map(connection)
        except Exception as e:
            bt.logging.error(
                f"in update_miner_rewards (got an exception): {e}"
            )
            traceback.print_exc(file=sys.stderr)
            return None

        for row in miner_data:
            miner_id = row["miner_id"]
            row["miner_uid"] = (
                miner_uid_map[miner_id] if miner_id in miner_uid_map else None
            )

        return miner_data

    def update_miner_rewards(self, miner_rewards_data: list[dict]):
        try:
            with self.engine.connect() as connection:
                with connection.begin():
                    insert_stmt = insert(miner_rewards).values(
                        miner_rewards_data
                    )
                    connection.execute(insert_stmt)
        except Exception as e:
            bt.logging.error(
                f"in update_miner_rewards (got an exception): {e}"
            )
            traceback.print_exc(file=sys.stderr)

    def update_weights_history(
        self,
        miner_uids: list[int],
        miner_weights: list[float],
        norm_miner_uids: list[str],
        norm_miner_weights: list[str],
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
                with connection.begin():
                    insert_stmt = insert(weights_update_history).values(
                        update_weights_rows
                    )
                    connection.execute(insert_stmt)
        except Exception as e:
            bt.logging.error(
                f"in update_weights_history (got an exception): {e}"
            )
            traceback.print_exc(file=sys.stderr)
