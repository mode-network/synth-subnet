from typing import Optional, Union
import aiohttp


import bittensor as bt
from bittensor_wallet import Keypair, Wallet


class SynthDendrite(bt.Dendrite):
    def __init__(self, wallet: Optional[Union[Wallet, Keypair]] = None):
        super().__init__(wallet=wallet)

    def process_server_response(
        self,
        server_response: aiohttp.ClientResponse,
        json_response: dict,
        local_synapse: bt.Synapse,
    ):
        bt.logging.trace("skipping dendrite processing for synth dendrite")
