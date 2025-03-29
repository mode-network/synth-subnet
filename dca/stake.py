import asyncio
import logging
from typing import List, Dict
from traceback import print_exception
import os

from dotenv import load_dotenv
import bittensor as bt
from bittensor.core.async_subtensor import get_async_subtensor

from dca.utils import (
    logger,
    compute_weights_from_ranks,
    get_subnet_stats,
    print_table_rich,
    read_config,
    read_ranks_file,
)

# Global variable to record the total TAO allocated by the script.
TOTAL_ALLOCATED = 0.0


async def stake(
    wallet: bt.wallet,
    delay: int,
    validator: str,
    allowed_subnets: List[int],
    weight_dict: Dict[int, float],
    amount_staked: float,
    drive: float,
):
    global TOTAL_ALLOCATED
    sub = await get_async_subtensor("finney")
    try:
        stats, rank_dict = await get_subnet_stats(
            sub, allowed_subnets, weight_dict, drive
        )
        valid_subnets = [
            netuid for netuid in allowed_subnets if netuid in stats
        ]
        if not valid_subnets:
            logger.warning("No allowed subnets with valid stats found.")
            return

        scores = {netuid: stats[netuid]["score"] for netuid in valid_subnets}
        for netuid, score in scores.items():
            logger.info(
                f"Subnet {netuid} - Yield: {stats[netuid]['raw_yield']:.4f}, Boost: {stats[netuid]['boost']:.4f}, Score: {score:.4f}"
            )

        best_subnet = max(valid_subnets, key=lambda x: scores[x])
        logger.info(
            f"Staking into subnet: {best_subnet} (Score: {scores[best_subnet]:.4f})"
        )

        # Stake into the best subnet.
        try:
            await sub.add_stake(
                wallet=wallet,
                hotkey_ss58=validator,
                netuid=best_subnet,
                amount=bt.Balance.from_tao(amount_staked),
                wait_for_inclusion=False,
                wait_for_finalization=False,
            )
            logger.info(
                f"Staked {amount_staked:.4f} TAO to subnet {best_subnet}"
            )
            TOTAL_ALLOCATED += amount_staked
        except Exception as err:
            logger.error(f"Failed to stake on subnet {best_subnet}: {err}")
            print_exception(type(err), err, err.__traceback__)

        # Retrieve updated stake info.
        stake_info = await sub.get_stake_for_coldkey_and_hotkey(
            hotkey_ss58=validator,
            coldkey_ss58=wallet.coldkey.ss58_address,
            netuids=allowed_subnets,
        )
        balance = float(
            await sub.get_balance(address=wallet.coldkey.ss58_address)
        )
        print_table_rich(
            stake_info,
            allowed_subnets,
            stats,
            rank_dict,
            balance,
            TOTAL_ALLOCATED,
        )

        # logger.info("Waiting for the next block...")
        # await sub.wait_for_block()

        logger.info(f"waiting for {delay} seconds")
        await asyncio.sleep(delay)
    finally:
        await sub.close()


async def main():
    config = read_config()
    load_dotenv()

    wallet_name = config["wallet"]
    delay = config["delay"]
    amount_staked = config["amount_staked"]
    validator = config["validator"]
    ranks_file = config["ranks_file"]
    ranking_beta = config["ranking_beta"]
    drive = config.get("drive", 1.0)

    bt.logging._logger.addHandler(logging.FileHandler("log-stake.log"))
    print("file logger added")

    ranks = read_ranks_file(ranks_file)
    weight_dict = compute_weights_from_ranks(ranks, ranking_beta)
    allowed_subnets = ranks

    logger.info(
        f"Starting staking service with {amount_staked:.4f} TAO staked"
    )
    logger.info(f"Allowed subnets (from ranking): {allowed_subnets}")
    logger.info(f"Computed weights: {weight_dict}")

    wallet = bt.wallet(name=wallet_name)
    wallet.create_if_non_existent()
    password = os.getenv("BT_PW")
    if password is not None:
        wallet.coldkey_file.save_password_to_env(os.getenv("BT_PW"))
    wallet.unlock_coldkey()
    logger.info(f"Using wallet: {wallet.name}")

    while True:
        try:
            await stake(
                wallet,
                delay,
                validator,
                allowed_subnets,
                weight_dict,
                amount_staked,
                drive,
            )
        except Exception as e:
            logger.error(f"Error in main loop: {e}")
            await asyncio.sleep(12)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Service stopped by user.")
    except Exception as e:
        print_exception(type(e), e, e.__traceback__)
        logger.critical(f"Critical error: {e}")
