module.exports = {
  apps: [
    {
      name: 'validator',
      interpreter: 'python3',
      script: './neurons/validator.py',
      args: '--netuid 50 --logging.debug --logging.trace --wallet.name validator --wallet.hotkey default --neuron.axon_off true --neuron.vpermit_tao_limit 1000 --ewma.alpha 2.0 --ewma.half_life_days 1.0 --ewma.cutoff_days 2',
      env: {
        PYTHONPATH: '.',
      },
    },
  ],
};
