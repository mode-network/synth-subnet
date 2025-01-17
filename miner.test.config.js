module.exports = {
  apps: [
    {
      name: 'miner',
      script: 'python3',
      args: './neurons/miner.py --netuid 247 --logging.debug --logging.trace --subtensor.network test --wallet.name miner --wallet.hotkey default --axon.port 8091 --blacklist.validator.min_stake 0',
      env: {
        PYTHONPATH: '.'
      },
    },
  ],
};
