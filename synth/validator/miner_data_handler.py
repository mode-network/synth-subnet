from datetime import datetime, timedelta
import traceback
import sys
import typing
import logging
import math


import bittensor as bt
import pandas as pd
from sqlalchemy import (
    join,
    literal_column,
    Connection,
    Engine,
    and_,
    exists,
    select,
    func,
    desc,
    not_,
    update,
)
from sqlalchemy.dialects.postgresql import insert
from tenacity import (
    before_log,
    retry,
    stop_after_attempt,
    wait_random_exponential,
)


from synth.db.models import (
    MinerPrediction,
    Miner,
    MinerScore,
    ValidatorRequest,
    MetagraphHistory,
    MinerReward,
    get_engine,
    WeightsUpdateHistory,
)
from synth.simulation_input import SimulationInput
from synth.validator import response_validation_v2


class MinerDataHandler:
    def __init__(self, engine: typing.Optional[Engine] = None):
        # Use the provided engine or fall back to the default engine
        self.engine = engine or get_engine()

    def get_miner_uids(self, connection: Connection):
        ranked_miners = select(
            Miner,
            func.row_number()
            .over(
                partition_by=Miner.miner_uid,
                order_by=desc(Miner.updated_at),
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

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_random_exponential(multiplier=7),
        reraise=True,
        before=before_log(bt.logging._logger, logging.DEBUG),
    )
    def save_responses(
        self,
        miner_predictions: dict,
        simulation_input: SimulationInput,
        request_time: datetime,
    ):
        """Save miner predictions and simulation input."""

        # Prepare the ValidatorRequest row from the simulation input:
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
                    # Insert into ValidatorRequest and get its ID
                    insert_stmt_validator = insert(ValidatorRequest).values(
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
                                    == response_validation_v2.CORRECT
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
                        MinerPrediction
                    ).values(miner_prediction_records)
                    connection.execute(insert_stmt_miner_predictions)
            return validator_requests_id  # TODO: finish this: refactor to add the validator_requests_id in the score and reward table
        except Exception as e:
            bt.logging.error(f"in save_responses (got an exception): {e}")
            traceback.print_exc(file=sys.stderr)

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_random_exponential(multiplier=7),
        reraise=True,
        before=before_log(bt.logging._logger, logging.DEBUG),
    )
    def set_miner_scores(
        self,
        real_prices: list[dict],
        validator_requests_id: int,
        reward_details: list[dict],
        scored_time: datetime,
    ):
        try:
            with self.engine.connect() as connection:
                with connection.begin():
                    # update validator request with the real paths
                    if real_prices is not None and len(real_prices) > 0:
                        real_prices = [
                            (
                                None
                                if (isinstance(x, float) and math.isnan(x))
                                else x
                            )
                            for x in real_prices
                        ]
                        update_stmt_validator = (
                            update(ValidatorRequest)
                            .where(
                                ValidatorRequest.id == validator_requests_id
                            )
                            .values(
                                {
                                    "real_prices": real_prices,
                                }
                            )
                        )
                        connection.execute(update_stmt_validator)

                    rows_to_insert = []
                    for row in reward_details:
                        rows_to_insert.append(
                            {
                                "miner_uid": row["miner_uid"],  # deprecated
                                "scored_time": scored_time.isoformat(),
                                "miner_predictions_id": row[
                                    "miner_prediction_id"
                                ],
                                "score_details_v3": {
                                    "total_crps": row["total_crps"],
                                    "percentile90": row["percentile90"],
                                    "lowest_score": row["lowest_score"],
                                    "prompt_score_v3": row["prompt_score_v3"],
                                    "crps_data": row["crps_data"],
                                },
                                "prompt_score_v3": row["prompt_score_v3"],
                            }
                        )
                    insert_stmt_miner_scores = (
                        insert(MinerScore)
                        .values(rows_to_insert)
                        .on_conflict_do_update(
                            constraint="uq_miner_scores_miner_predictions_id",
                            set_={
                                "score_details_v3": {
                                    "total_crps": row["total_crps"],
                                    "percentile90": row["percentile90"],
                                    "lowest_score": row["lowest_score"],
                                    "prompt_score_v3": row["prompt_score_v3"],
                                    "crps_data": row["crps_data"],
                                },
                                "prompt_score_v3": row["prompt_score_v3"],
                            },
                        )
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
                        Miner.miner_uid,
                    )
                    .select_from(MinerPrediction)
                    .join(
                        Miner,
                        Miner.id == MinerPrediction.miner_id,
                    )
                    .where(
                        MinerPrediction.validator_requests_id
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

    def get_miner_prediction(
        self, miner_uid: int, validator_request_id: int
    ) -> typing.Optional[MinerPrediction]:
        """Retrieve the record with the longest valid interval for the given miner_id."""
        try:
            with self.engine.connect() as connection:
                query = (
                    select(
                        MinerPrediction.id,
                        MinerPrediction.prediction,
                        MinerPrediction.format_validation,
                        MinerPrediction.process_time,
                    )
                    .select_from(MinerPrediction)
                    .join(
                        Miner,
                        Miner.id == MinerPrediction.miner_id,
                    )
                    .where(
                        Miner.miner_uid == miner_uid,
                        MinerPrediction.validator_requests_id
                        == validator_request_id,
                    )
                    .limit(1)
                )

                result = MinerPrediction()
                row = connection.execute(query).fetchone()
                if row is not None:
                    result.id = row.id
                    result.prediction = row.prediction
                    result.format_validation = row.format_validation
                    result.process_time = row.process_time

            return result
        except Exception as e:
            bt.logging.error(
                f"in get_miner_prediction (got an exception): {e}"
            )
            traceback.print_exc(file=sys.stderr)
            return None

    def get_latest_prediction_requests(
        self,
        scored_time: datetime,
        cutoff_days: int,
    ) -> typing.Optional[list[ValidatorRequest]]:
        """
        Retrieve the list of IDs of the latest validator requests that (start_time + time_length) < scored_time
        and (start_time + time_length) >= scored_time - cutoff_days.
        This is to ensure that we only get requests that are within the cutoff_days.
        and exclude records that are already scored
        """
        try:
            with self.engine.connect() as connection:
                subq = (
                    select(1)
                    .select_from(
                        join(
                            MinerPrediction,
                            MinerScore,
                            MinerPrediction.id
                            == MinerScore.miner_predictions_id,
                        )
                    )
                    .where(
                        and_(
                            MinerPrediction.validator_requests_id
                            == ValidatorRequest.id,
                            MinerScore.prompt_score_v3.isnot(None),
                        )
                    )
                )

                window_start = (
                    ValidatorRequest.start_time
                    + literal_column("INTERVAL '1 second'")
                    * ValidatorRequest.time_length
                )

                query = (
                    select(
                        ValidatorRequest.id,
                        ValidatorRequest.start_time,
                        ValidatorRequest.asset,
                        ValidatorRequest.time_length,
                        ValidatorRequest.time_increment,
                    )
                    .where(
                        and_(
                            # Compare start_time plus an interval (in seconds) to the scored_time.
                            window_start < scored_time,
                            # Compare start_time plus an interval (in seconds) to the cutoff_days.
                            # This is to ensure that we only get requests that are within the cutoff_days.
                            # Because we want to include in the moving average only the requests that are within the cutoff_days.
                            window_start
                            >= scored_time - timedelta(days=cutoff_days),
                            # Exclude records that have a matching miner_prediction via the NOT EXISTS clause.
                            not_(exists(subq)),
                        )
                    )
                    .order_by(ValidatorRequest.start_time.asc())
                )

                results: list[ValidatorRequest] = []
                for row in connection.execute(query).fetchall():
                    vr = ValidatorRequest()
                    vr.id = row.id
                    vr.start_time = row.start_time
                    vr.asset = row.asset
                    vr.time_length = row.time_length
                    vr.time_increment = row.time_increment
                    results.append(vr)

                return results
        except Exception as e:
            bt.logging.error(
                f"in get_latest_prediction_request (got an exception): {e}"
            )
            traceback.print_exc(file=sys.stderr)
            return None

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_random_exponential(multiplier=7),
        reraise=True,
        before=before_log(bt.logging._logger, logging.DEBUG),
    )
    def insert_new_miners(self, metagraph_info: list):
        """Insert or update miners table with the provided data."""
        try:
            with self.engine.connect() as connection:
                with connection.begin():
                    insert_stmt = (
                        insert(Miner)
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
            bt.logging.error(f"in insert_new_miners (got an exception): {e}")
            traceback.print_exc(file=sys.stderr)

    def update_metagraph_history(self, metagraph_info: list):
        try:
            with self.engine.connect() as connection:
                with connection.begin():
                    insert_stmt = insert(MetagraphHistory).values(
                        metagraph_info
                    )
                    connection.execute(insert_stmt)
        except Exception as e:
            bt.logging.error(
                f"in update_metagraph_history (got an exception): {e}"
            )
            traceback.print_exc(file=sys.stderr)

    def get_miner_scores(self, scored_time: datetime, cutoff_days: int):
        min_scored_time = scored_time - timedelta(days=cutoff_days)

        try:
            with self.engine.connect() as connection:
                query = (
                    select(
                        MinerPrediction.miner_id,
                        MinerScore.prompt_score_v3,
                        MinerScore.scored_time,
                        MinerScore.score_details_v3,
                        ValidatorRequest.asset,
                        ValidatorRequest.start_time,
                    )
                    .select_from(MinerScore)
                    .join(
                        MinerPrediction,
                        MinerPrediction.id == MinerScore.miner_predictions_id,
                    )
                    .join(
                        ValidatorRequest,
                        ValidatorRequest.id
                        == MinerPrediction.validator_requests_id,
                    )
                    .where(MinerScore.scored_time > min_scored_time)
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
                f"in populate_miner_uid_in_miner_data (got an exception): {e}"
            )
            traceback.print_exc(file=sys.stderr)
            return None

        for row in miner_data:
            miner_id = row["miner_id"]
            row["miner_uid"] = (
                miner_uid_map[miner_id] if miner_id in miner_uid_map else None
            )

        return miner_data

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_random_exponential(multiplier=7),
        reraise=True,
        before=before_log(bt.logging._logger, logging.DEBUG),
    )
    def update_miner_rewards(self, miner_rewards_data: list[dict]):
        try:
            with self.engine.connect() as connection:
                with connection.begin():
                    insert_stmt = insert(MinerReward).values(
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
        scored_time: datetime,
    ):
        update_weights_rows = {
            "miner_uids": miner_uids,
            "miner_weights": miner_weights,
            "norm_miner_uids": norm_miner_uids,
            "norm_miner_weights": norm_miner_weights,
            "update_result": update_result,
            "updated_at": scored_time.isoformat(),
        }

        try:
            with self.engine.connect() as connection:
                with connection.begin():
                    insert_stmt = insert(WeightsUpdateHistory).values(
                        update_weights_rows
                    )
                    connection.execute(insert_stmt)
        except Exception as e:
            bt.logging.error(
                f"in update_weights_history (got an exception): {e}"
            )
            traceback.print_exc(file=sys.stderr)
