import bittensor as bt

metagraph = bt.subtensor("finney").metagraph(netuid=247)

print(metagraph.R)
print(metagraph.coldkeys)
print(metagraph.hotkeys)
