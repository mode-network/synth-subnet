import bittensor as bt

from simulation.api.get_query_axons import get_query_api_axons
from simulation.api.synth import SynthAPI

import asyncio

from simulation.simulation_input import SimulationInput
from simulation.utils.helpers import get_current_time


# Example usage
async def test_prediction():
    wallet = bt.wallet()

    # Fetch the axons of the available API nodes, or specify UIDs directly
    metagraph = bt.subtensor("finney").metagraph(netuid=247)

    uids = [uid.item() for uid in metagraph.uids if metagraph.trust[uid] > 0]

    axons = await get_query_api_axons(
        wallet=wallet, metagraph=metagraph, uids=uids
    )

    current_time = get_current_time()

    simulation_input = SimulationInput(
        asset="BTC",
        start_time=current_time,
        time_increment=300,
        time_length=86400,
        num_simulations=1
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
    asyncio.run(test_prediction())
