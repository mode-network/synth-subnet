module.exports = {
  apps: [
    {
      name: 'validator-dev',
      script: 'python3',
      args: './neurons/validator.py --netuid 1 --logging.debug --logging.trace --subtensor.chain_endpoint ws://127.0.0.1:9946 --wallet.name validator --wallet.hotkey default --neuron.axon_off true',
      env: {
        PYTHONPATH: '.',
      },
    },
  ],
};
