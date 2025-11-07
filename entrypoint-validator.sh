#!/bin/bash

default_network=finney
network="${NETWORK:-$default_network}"
netuid=50

vpermit_tao_limit=999999

validator_coldkey_name=validator
validator_hotkey_name=default

ewma_window_days=10
ewma_cutoff_days=10
softmax_beta=-0.1

log_id_prefix=my_validator_name

python3.10 ./neurons/validator.py \
		--wallet.name $validator_coldkey_name \
		--wallet.hotkey $validator_hotkey_name \
		--subtensor.network $network \
		--netuid $netuid \
		--logging.debug \
		--neuron.axon_off true \
		--ewma.window_days $ewma_window_days \
		--ewma.cutoff_days $ewma_cutoff_days \
		--softmax.beta $softmax_beta \
		--neuron.vpermit_tao_limit $vpermit_tao_limit \
		--gcp.log_id_prefix $log_id_prefix \
