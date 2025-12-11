module.exports = {
  apps: [
    {
      name: "validator",
      interpreter: "python3",
      script: "./neurons/validator.py",
      args: "--netuid 50 --logging.debug --wallet.name validator --wallet.hotkey default --neuron.axon_off true --neuron.vpermit_tao_limit 999999 --softmax.beta -0.2",
      env: {
        PYTHONPATH: ".",
      },
    },
  ],
};
