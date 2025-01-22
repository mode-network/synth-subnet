module.exports = {
  apps: [
    {
      name: 'validator',
      script: 'python3',
      args: './neurons/validator.py --netuid 247 --logging.debug --logging.trace --subtensor.network test --wallet.name validator --wallet.hotkey default --neuron.axon_off true --ewma.alpha 2.0 --ewma.half_life_days 1.0 --ewma.cutoff_days 2',
      env: {
        PYTHONPATH: '.',
      },
    },
  ],
};
