module.exports = {
  apps: [
    {
      name: "validator",
      interpreter: "python3",
      script: "./neurons/validator.py",
      args: "--netuid 247 --logging.debug --logging.trace --subtensor.network test --wallet.name validator --wallet.hotkey default --neuron.axon_off true --ewma.window_days 10 --ewma.cutoff_days 10 --softmax.beta -0.1",
      env: {
        PYTHONPATH: ".",
      },
    },
  ],
};
