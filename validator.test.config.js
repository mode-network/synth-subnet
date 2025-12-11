module.exports = {
  apps: [
    {
      name: "validator",
      interpreter: "python3",
      script: "./neurons/validator.py",
      args: "--netuid 247 --logging.debug --logging.trace --subtensor.network test --wallet.name validator --wallet.hotkey default --neuron.axon_off true --softmax.beta -0.2",
      env: {
        PYTHONPATH: ".",
      },
    },
  ],
};
