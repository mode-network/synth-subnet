module.exports = {
  apps: [
    {
      name: 'validator-local',
      script: 'python3',
      args: './neurons/validator.py --netuid 1 --logging.debug --logging.trace --subtensor.chain_endpoint ws://127.0.0.1:9946 --wallet.name validator --wallet.hotkey default --prediction_history_file prediction_history_local.json'
    },
  ],
};
