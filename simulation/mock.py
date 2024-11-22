from dataclasses import dataclass
from typing import Any, Optional, Tuple, Union
from unittest.mock import MagicMock

import torch
import asyncio
import bittensor as bt
from bittensor.utils.balance import Balance
from bittensor.core.threadpool import PriorityThreadPoolExecutor


@dataclass
class MockWallet:
    hotkey: Any
    coldkey: Any = None
    coldkeypub: Any = None


class MockSubtensor(bt.MockSubtensor):
    def __init__(self, netuid, n=16, wallet=None, network="mock"):
        super().__init__(network=network)

        if not self.subnet_exists(netuid):
            self.create_subnet(netuid)

        self.is_hotkey_registered = MagicMock(
            return_value=True
        )
    
        # Register ourself (the verifier) as a neuron at uid=0
        if wallet is not None:
            self.force_register_neuron(
                netuid=netuid,
                wallet=wallet,
                balance=100000,
                stake=100000,
            )

        # Register n mock neurons who will be provers
        for i in range(1, n + 1):
            self.force_register_neuron(
                netuid=netuid,
                wallet=wallet,
                balance=100000,
                stake=100000,
            )

    def force_register_neuron(
        self,
        netuid: int,
        wallet: MockWallet,
        stake: Union["Balance", float, int] = Balance(0),
        balance: Union["Balance", float, int] = Balance(0),
    ) -> int:
        """
        Force register a neuron on the mock chain, returning the UID.
        """
        stake = self._convert_to_balance(stake)
        balance = self._convert_to_balance(balance)

        subtensor_state = self.chain_state["SubtensorModule"]
        if netuid not in subtensor_state["NetworksAdded"]:
            raise Exception("Subnet does not exist")

        uid = self.register(wallet=wallet, netuid=netuid)

        subtensor_state["TotalStake"][self.block_number] = (
            self._get_most_recent_storage(subtensor_state["TotalStake"]) + stake.rao
        )
        # subtensor_state["Stake"][wallet.hotkey][wallet.coldkey][self.block_number] = stake.rao

        # if balance.rao > 0:
        #     self.force_set_balance(wallet.coldkey, balance)
        # self.force_set_balance(wallet.coldkey, balance)

        return uid


class MockDendriteResponse:
    class mock_status:
        status_code = 200

    completion = ""


class MockDendrite(torch.nn.Module):
    async def query(self, synapse, axons, timeout):
        async def test():
            await asyncio.sleep(0.01)
            return [MockDendriteResponse(synapse.messages[0]) for _ in axons]

        return await test()

    def resync(self, metagraph):
        pass

class MockMetagraph(bt.Metagraph):
    def __init__(self, netuid=1, network="mock", subtensor=None):
        super().__init__(netuid=netuid, network=network, sync=False)

        self._assign_neurons = MagicMock()
        self._set_metagraph_attributes = MagicMock()
        self._set_weights_and_bonds = MagicMock()

        if subtensor is not None:
            self.subtensor = subtensor
        self.sync(subtensor=subtensor)

        for axon in self.axons:
            axon.ip = "127.0.0.0"
            axon.port = 8091

        bt.logging.info(f"Metagraph: {self}")
        bt.logging.info(f"Axons: {self.axons}")


class MockHotkey:
    def __init__(self, ss58_address):
        self.ss58_address = ss58_address
        self.public_key = bytes(ss58_address, "utf-8")

    def sign(self, *args, **kwargs):
        return f"Signed: {args!r} {kwargs!r}".encode()


class MockInfo:
    def to_string(self):
        return "MockInfoString"


class AxonMock:
    def __init__(self):
        self.status_code = None
        self.forward_class_types = {}
        self.blacklist_fns = {}
        self.priority_fns = {}
        self.forward_fns = {}
        self.verify_fns = {}
        self.thread_pool = PriorityThreadPoolExecutor(max_workers=1)


class SynapseMock(bt.Synapse):
    pass

