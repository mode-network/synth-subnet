#!/bin/bash

network=finney
netuid=50

vpermit_tao_limit=999999

validator_coldkey_name=validator
validator_hotkey_name=default

ewma_half_life_days=3.5
ewma_cutoff_days=7
softmax_beta=-0.0475

project_id=my_project_id
log_id_prefix=my_validator_name

python3.10 ./neurons/validator.py \
		--wallet.name $validator_coldkey_name \
		--wallet.hotkey $validator_hotkey_name \
		--subtensor.network $network \
		--netuid $netuid \
		--logging.debug \
		--neuron.axon_off true \
		--ewma.half_life_days $ewma_half_life_days \
		--ewma.cutoff_days $ewma_cutoff_days \
		--ewma.standard_ma_disabled \
		--softmax.beta $softmax_beta \
		--neuron.vpermit_tao_limit $vpermit_tao_limit \
		--gcp.project_id $project_id \
		--gcp.log_id_prefix $log_id_prefix \
