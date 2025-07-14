module.exports = {
  apps: [
    {
      name: "miner-dev",
      interpreter: "python3",
      script: "./neurons/miner.py",
      args: "--netuid 1 --logging.debug --logging.trace --subtensor.chain_endpoint ws://127.0.0.1:9946 --wallet.name miner --wallet.hotkey default --axon.port 8091",
      env: {
        PYTHONPATH: ".",
      },
    },
  ],
};
