import asyncio
import time
from typing import Optional
import bittensor as bt
from bittensor.utils.balance import rao
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DTAOBuyer:
    def __init__(
        self,
        wallet_name: str = "miner",
        wallet_hotkey: str = "default",
        amount_per_block_rao: int = 1_000_000,  # 0.001 TAO in rao
        max_purchases: Optional[int] = None,  # None for infinite
        network: str = "mainnet"
    ):
        self.wallet = bt.wallet(name=wallet_name, hotkey=wallet_hotkey)
        self.subtensor = bt.subtensor(network=network)
        self.amount_per_block = rao(amount_per_block_rao)
        self.max_purchases = max_purchases
        self.purchases_made = 0
        
    async def wait_for_next_block(self):
        """Wait for the next block to be produced"""
        current_block = self.subtensor.get_current_block()
        while self.subtensor.get_current_block() == current_block:
            await asyncio.sleep(1)
        return self.subtensor.get_current_block()

    async def buy_dtao(self):
        """Executes a single dTAO purchase"""
        try:
            # Get current balance before purchase
            balance_before = self.subtensor.get_balance(self.wallet.coldkeypub.ss58_address)
            
            # Execute purchase
            success = await self.subtensor.buy_dtao(
                wallet=self.wallet,
                amount=self.amount_per_block,
                prompt=False  # Don't ask for confirmation
            )
            
            if success:
                # Get balance after purchase to calculate actual cost
                balance_after = self.subtensor.get_balance(self.wallet.coldkeypub.ss58_address)
                cost = balance_before - balance_after
                
                logger.info(
                    f"Successfully purchased {self.amount_per_block} rao of dTAO "
                    f"(Cost: {cost} rao)"
                )
                self.purchases_made += 1
                return True
            else:
                logger.error("Purchase failed")
                return False
                
        except Exception as e:
            logger.error(f"Error during purchase: {str(e)}")
            return False

    async def run(self):
        """Main loop to continuously purchase dTAO"""
        logger.info(
            f"Starting dTAO buyer with {self.amount_per_block} rao per block. "
            f"Max purchases: {self.max_purchases if self.max_purchases else 'unlimited'}"
        )
        
        while True:
            if self.max_purchases and self.purchases_made >= self.max_purchases:
                logger.info(f"Reached maximum number of purchases ({self.max_purchases})")
                break
                
            try:
                current_block = await self.wait_for_next_block()
                logger.info(f"New block: {current_block}")
                
                # Check if we have enough balance for the purchase
                balance = self.subtensor.get_balance(self.wallet.coldkeypub.ss58_address)
                if balance < self.amount_per_block:
                    logger.warning(
                        f"Insufficient balance: {balance} rao "
                        f"(needed: {self.amount_per_block} rao)"
                    )
                    continue
                
                await self.buy_dtao()
                
            except KeyboardInterrupt:
                logger.info("Stopping due to user interrupt")
                break
            except Exception as e:
                logger.error(f"Error in main loop: {str(e)}")
                await asyncio.sleep(1)  # Prevent tight loop on errors

def main():
    # Configure your parameters here
    buyer = DTAOBuyer(
        wallet_name="miner",          # Your wallet name
        wallet_hotkey="default",      # Your hotkey name
        amount_per_block_rao=1_000_000,  # Amount to buy per block (in rao)
        max_purchases=None,           # None for unlimited
        network="mainnet"            # Network to use
    )
    
    try:
        asyncio.run(buyer.run())
    except KeyboardInterrupt:
        logger.info("Script stopped by user")
    except Exception as e:
        logger.error(f"Script error: {str(e)}")

if __name__ == "__main__":
    main()
