import asyncio
from typing import List, Dict
from traceback import print_exception
import os

from dotenv import load_dotenv
import bittensor as bt
from bittensor.core.async_subtensor import get_async_subtensor

from dca.utils import (
    compute_weights_from_ranks,
    get_subnet_stats,
    print_table_rich,
    read_config,
    read_ranks_file,
    logger,
)

# Global variable to record the total TAO unstaked by the script.
TOTAL_UNSTAKED = 0.0


async def unstake(
    wallet: bt.wallet,
    delay: int,
    validator: str,
    netuid: int,
    allowed_subnets: List[int],
    weight_dict: Dict[int, float],
    amount_unstaked: float,
    drive: float,
):
    global TOTAL_UNSTAKED
    sub = await get_async_subtensor("finney")
    try:
        stats, rank_dict = await get_subnet_stats(
            sub, allowed_subnets, weight_dict, drive
        )

        logger.info(f"Unstaking from subnet: {netuid}, validator: {validator}")
        logger.info(f"Unstaking {amount_unstaked:.4f} TAO")

        try:
            await sub.unstake(
                wallet=wallet,
                hotkey_ss58=validator,
                netuid=netuid,
                amount=bt.Balance.from_tao(0.01),
                wait_for_inclusion=False,
                wait_for_finalization=False,
            )
            logger.info(
                f"Unstaked {amount_unstaked:.4f} TAO to subnet {netuid}"
            )
            TOTAL_UNSTAKED += amount_unstaked
        except Exception as err:
            logger.error(f"Failed to stake on subnet {netuid}: {err}")
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
            TOTAL_UNSTAKED,
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
    amount_unstaked = config["amount_unstaked"]
    validator = config["validator"]
    netuid = config["netuid"]
    ranks_file = config["ranks_file"]
    ranking_beta = config["ranking_beta"]
    drive = config.get("drive", 1.0)

    ranks = read_ranks_file(ranks_file)
    weight_dict = compute_weights_from_ranks(ranks, ranking_beta)
    allowed_subnets = ranks

    logger.info(
        f"Starting unstaking service with {amount_unstaked:.4f} TAO staked"
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
            await unstake(
                wallet,
                delay,
                validator,
                netuid,
                allowed_subnets,
                weight_dict,
                amount_unstaked,
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
