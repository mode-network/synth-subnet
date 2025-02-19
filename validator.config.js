module.exports = {
  apps: [
    {
      name: 'validator',
      interpreter: 'python3',
      script: './neurons/validator.py',
      args: '--netuid 50 --logging.debug --wallet.name validator --wallet.hotkey default --neuron.axon_off true --neuron.vpermit_tao_limit 999999 --ewma.alpha 4.0 --ewma.half_life_days 2.0 --ewma.cutoff_days 4 --softmax.beta -0.002 --ewma.standard_ma_disabled',
      env: {
        PYTHONPATH: '.',
      },
    },
  ],
};
