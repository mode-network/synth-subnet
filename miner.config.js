module.exports = {
  apps: [
    {
      name: 'miner',
      interpreter: 'python3',
      script: './neurons/miner.py',
      args: '--netuid 247 --logging.debug --logging.trace --wallet.name miner --wallet.hotkey default --axon.port 8091 --blacklist.force_validator_permit true --blacklist.validator_min_stake 1000',
      env: {
        PYTHONPATH: '.'
      },
    },
  ],
};
