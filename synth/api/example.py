import bittensor
import bittensor as bt

from synth.api.get_query_axons import get_query_api_axons
from synth.api.synth import SynthAPI

import asyncio

from synth.simulation_input import SimulationInput
from synth.utils.helpers import get_current_time


# Example usage
async def test_prediction(args):
    wallet_name = bittensor.wallet(name=args.name)
    wallet = bt.wallet(wallet_name.name)

    # Fetch the axons of the available API nodes, or specify UIDs directly
    metagraph = bt.subtensor("test").metagraph(netuid=247)

    uids = [uid.item() for uid in metagraph.uids if metagraph.trust[uid] > 0]

    axons = await get_query_api_axons(
        wallet=wallet, metagraph=metagraph, uids=uids
    )

    current_time = get_current_time()

    simulation_input = SimulationInput(
        asset="BTC",
        start_time=current_time.isoformat(),
        time_increment=300,
        time_length=86400,
        num_simulations=1,
    )
    bt.logging.info(f"Sending {str(simulation_input)} to predict a path.")

    synth = SynthAPI(wallet)
    response = await synth(
        axons=axons,
        simulation_input=simulation_input,
        timeout=100,
    )

    print(response)
    print(uids)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Generate a signature")
    parser.add_argument("--message", help="The message to sign", type=str)
    parser.add_argument("--name", help="The wallet name", type=str)
    args = parser.parse_args()

    asyncio.run(test_prediction(args))
