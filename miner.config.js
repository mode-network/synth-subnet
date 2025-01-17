module.exports = {
  apps: [
    {
      name: 'miner',
      script: 'python3',
      args: './neurons/miner.py --netuid 247 --logging.debug --logging.trace --wallet.name miner --wallet.hotkey default --axon.port 8091',
      env: {
        PYTHONPATH: '.'
      },
    },
  ],
};
