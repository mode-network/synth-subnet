from datetime import datetime, timedelta


import bittensor as bt
from dotenv import load_dotenv


from synth.validator import prompt_config
from synth.validator.miner_data_handler import MinerDataHandler
from synth.validator.price_data_provider import PriceDataProvider
from synth.validator.reward import (
    get_rewards_multiprocess,
    print_scores_df,
)


load_dotenv()


def backfill_scores(prompt: prompt_config.PromptConfig):
    price_data_provider = PriceDataProvider()
    miner_data_handler = MinerDataHandler()
    nprocs = 8

    scored_time_start = datetime.fromisoformat("2026-03-28 17:10:00")
    scored_time_end = datetime.fromisoformat("2026-04-10")

    validator_requests = miner_data_handler.get_validator_requests_to_backfill(
        scored_time_start,
        scored_time_end,
        prompt.time_length,
    )

    fail_count = 0
    for validator_request in validator_requests:
        bt.logging.info(f"validator_request_id: {validator_request.id}")
        print(f"calculating for request: {validator_request}")

        prompt_scores, detailed_info, real_prices = get_rewards_multiprocess(
            miner_data_handler=miner_data_handler,
            price_data_provider=price_data_provider,
            validator_request=validator_request,
            nprocs=nprocs,
        )

        print_scores_df(prompt_scores, detailed_info)

        if prompt_scores is None:
            bt.logging.warning("No rewards calculated")
            fail_count += 1
            continue

        miner_score_time = validator_request.start_time + timedelta(
            seconds=int(validator_request.time_length)
        )

        miner_data_handler.set_miner_scores(
            real_prices,
            int(validator_request.id),
            detailed_info,
            miner_score_time,
        )

    print("report:")
    print("fail count", fail_count)


if __name__ == "__main__":
    backfill_scores(prompt_config.HIGH_FREQUENCY)
    # backfill_scores(prompt_config.LOW_FREQUENCY)
