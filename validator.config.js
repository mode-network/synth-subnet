module.exports = {
  apps: [
    {
      name: "validator",
      interpreter: "python3",
      script: "./neurons/validator.py",
      args: "--netuid 50 --logging.debug --wallet.name validator --wallet.hotkey default --neuron.axon_off true --neuron.vpermit_tao_limit 999999 --ewma.window_days 10 --ewma.cutoff_days 10 --softmax.beta -0.1",
      env: {
        PYTHONPATH: ".",
      },
    },
  ],
};
