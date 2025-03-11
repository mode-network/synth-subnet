#!/bin/bash

network=finney
netuid=50

vpermit_tao_limit=999999

validator_coldkey_name=validator
validator_hotkey_name=default

ewma_half_life_days=2.0
ewma_cutoff_days=4
softmax_beta=-0.003

wandb_enabled=false
wandb_project_name=my_wandb_project
wandb_entity=my_wandb_team

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
		--wandb.enabled $wandb_enabled \
		--wandb.project_name $wandb_project_name \
		--wandb.entity $wandb_entity
