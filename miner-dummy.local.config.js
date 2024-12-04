module.exports = {
  apps: [
    {
      name: 'miner-dummy-local',
      script: 'python3',
      args: './neurons/miner.py --netuid 1 --logging.debug --logging.trace --subtensor.chain_endpoint ws://127.0.0.1:9946 --wallet.name miner-dummy --wallet.hotkey default --axon.port 10000 --miner_type dummy'
    },
  ],
};
