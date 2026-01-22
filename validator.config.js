module.exports = {
  apps: [
    {
      name: "validator cycle low",
      interpreter: "python3",
      script: "./neurons/validator.py",
      args: "--netuid 50 --logging.debug --wallet.name validator --wallet.hotkey default --neuron.axon_off true --neuron.vpermit_tao_limit 999999 --neuron.nprocs 8 --validator.cycle_name low_frequency",
      env: {
        PYTHONPATH: ".",
      },
    },
    {
      name: "validator cycle high",
      interpreter: "python3",
      script: "./neurons/validator.py",
      args: "--netuid 50 --logging.debug --wallet.name validator --wallet.hotkey default --neuron.axon_off true --neuron.vpermit_tao_limit 999999 --neuron.nprocs 8 --validator.cycle_name high_frequency",
      env: {
        PYTHONPATH: ".",
      },
    },
    {
      name: "validator cycle scoring",
      interpreter: "python3",
      script: "./neurons/validator.py",
      args: "--netuid 50 --logging.debug --wallet.name validator --wallet.hotkey default --neuron.axon_off true --neuron.vpermit_tao_limit 999999 --neuron.nprocs 8 --validator.cycle_name scoring",
      env: {
        PYTHONPATH: ".",
      },
    },
  ],
};
