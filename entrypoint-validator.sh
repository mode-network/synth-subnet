#!/bin/bash

network=test
netuid=247

logging_level=debug
validator_coldkey_name=validator-base
validator_hotkey_name=default

ewma_alpha=2.0
ewma_half_life_days=1.0
ewma_cutoff_days=2

#source bt_venv/bin/activate

ls -la .
which python3.10

python3.10 ./neurons/validator.py \
		--wallet.name $validator_coldkey_name \
		--wallet.hotkey $validator_hotkey_name \
		--subtensor.network $network \
		--netuid $netuid \
		--logging.logging_level \
		--neuron.axon_off true \
		--ewma.alpha $ewma_alpha \
		--ewma.half_life_days $ewma_half_life_days \
		--ewma.cutoff_days $ewma_cutoff_days
