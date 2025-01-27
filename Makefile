# Network specific parameters
test_network = test
main_network = finney

testnet_netuid = 247
mainnet_netuid = 50


# Commands parameters
network = $(test_network)
netuid = $(testnet_netuid)

logging_level = debug
validator_coldkey_name = validator-base
validator_hotkey_name = default

ewma_alpha = 2.0
ewma_half_life_days = 1.0
ewma_cutoff_days = 2

# Python virtual environment
venv_python=bt_venv/bin/python3


# Commands
metagraph:
	btcli subnet metagraph --subtensor.network $(network) --netuid $(netuid)

validator:
	pm2 start -name validator -- ./neurons/validator.py \
		--wallet.name $(validator_coldkey_name) \
		--wallet.hotkey $(validator_hotkey_name) \
		--subtensor.network $(network) \
		--netuid $(netuid) \
		--logging.$(logging_level) \
		--neuron.axon_off true \
		--ewma.alpha $(ewma_alpha) \
		--ewma.half_life_days $(ewma_half_life_days) \
		--ewma.cutoff_days $(ewma_cutoff_days)
