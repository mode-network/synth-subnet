#!/bin/bash

default_network=finney
network="${NETWORK:-$default_network}"
netuid=50

vpermit_tao_limit=999999

validator_coldkey_name=validator
validator_hotkey_name=default

log_id_prefix=my_validator_name

python3.11 ./neurons/validator.py \
		--wallet.name $validator_coldkey_name \
		--wallet.hotkey $validator_hotkey_name \
		--subtensor.network $network \
		--netuid $netuid \
		--logging.debug \
		--neuron.axon_off true \
		--neuron.vpermit_tao_limit $vpermit_tao_limit \
		--gcp.log_id_prefix $log_id_prefix \
