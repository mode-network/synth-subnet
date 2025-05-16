import bittensor as bt
from substrateinterface import SubstrateInterface  # type: ignore

netuid = 50
subnet = bt.metagraph(netuid)
wallet = bt.wallet(name="validator", hotkey="default")
my_uid = subnet.hotkeys.index(wallet.hotkey.ss58_address)
print(f"Validator permit: {subnet.validator_permit[my_uid]}")

top_64_stake = sorted(subnet.S)[-64:]
print(
    f"Current requirement for validator permits based on the top 64 stake stands at {min(top_64_stake)} tao"
)

hotkey = "5Gy7xTzDwXYw4JV1TUUUt28PwUTQ95HBCYFJMwkaDmnbQUJ5"
network = "finney"
sub = bt.subtensor(network)
mg = sub.metagraph(netuid)
if hotkey not in mg.hotkeys:
    print(f"Hotkey {hotkey} deregistered")
else:
    print(f"Hotkey {hotkey} is registered")

substrate = SubstrateInterface(url="wss://entrypoint-finney.opentensor.ai:443")
result = substrate.query("SubtensorModule", "ValidatorPermit", [netuid])
print(result.value)
for uid, permit in enumerate(result.value):
    print(f"neuron uid {uid}: permit {permit}")
