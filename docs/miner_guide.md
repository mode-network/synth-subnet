# Miner Guide

### Table Of Contents

* [1. Create a Wallet](#1-create-a-wallet)
* [2. Run the Miner](#2-run-the-miner)
* [3. Appendix](#3-appendix)
  - [3.1. Useful Commands](#31-useful-commands)
  - [3.2. Running Multiple Miners](#32-running-multiple-miners)
  - [3.3. Troubleshooting](#33-troubleshooting)

## 1. Create a Wallet

> 💡 **TIP:** For a more extensive list of the Bittensor CLI commands see [here](https://docs.bittensor.com/btcli).

**Step 1: Activate the Python virtual environment**

If you haven't already, ensure you are running from the Python virtual environment:
```shell
source bt_venv/bin/activate
```

**Step 2: Create the cold/hot wallets**

```shell
btcli wallet create \
  --wallet.name miner \
  --wallet.hotkey default
```

> 🚨 **WARNING:** You must ensure your wallets have enough TAO (0.1 should be sufficient) to be start mining. For testnet, you can use the [`btcli wallet faucet`](https://docs.bittensor.com/btcli#btcli-wallet-faucet).

**Step 3: Register wallet**

Acquire a slot on the Bittensor subnet by registering the wallet:
```shell
btcli subnet register \
  --wallet.name miner \
  --wallet.hotkey default \
  --subtensor.network test \
  --netuid 247
```

**Step 4: Verify wallet registration (optional)**

Check the wallet has been registered:
```shell
btcli wallet overview \
  --wallet.name miner \
  --wallet.hotkey default \
  --subtensor.network test
```

You can also check the network metagraph:
```shell
btcli subnet metagraph \
  --subtensor.network test \
  --netuid 247
```

<sup>[Back to top ^][table-of-contents]</sup>

## 2. Run the Miner

**Step 1: Activate the Python virtual environment**

```shell
source bt_venv/bin/activate
```

**Step 2: Start PM2 with the miner config**

```shell
pm2 start miner.config.js
```

**Step 2: Check the miner is running (optional)**

```shell
pm2 list
```

<sup>[Back to top ^][table-of-contents]</sup>

## 3. Appendix

### 3.1. Useful Commands

| Command                      | Description                 |
|------------------------------|-----------------------------|
| `pm2 stop miner`             | Stops the miner.            |
| `pm2 logs miner --lines 100` | View the logs of the miner. |

<sup>[Back to top ^][table-of-contents]</sup>

### 3.2. Running Multiple Miners

In order to run multiple miners on the same machine, you must ensure that you correctly edit the `miner.config.js` file to allow for multiple apps. 

Each app **MUST** use a separate wallet (see [here](#1-create-a-wallet) for instructions on how to create a wallet) and it **MUST** use a different port for the `--axon.port` parameter in the start-up script.

An example for a config using multiple miners:

```js
// miner.config.js
module.exports = {
  apps: [
    {
      name: 'miner-1',
      script: 'python3',
      args: './neurons/miner.py --netuid 247 --logging.debug --logging.trace --subtensor.network test --wallet.name miner_1 --wallet.hotkey default --axon.port 8091',
      env: {
        PYTHONPATH: '.'
      },
    },
    {
      name: 'miner-2',
      script: 'python3',
      args: './neurons/miner.py --netuid 247 --logging.debug --logging.trace --subtensor.network test --wallet.name miner_2 --wallet.hotkey default --axon.port 8092',
      env: {
        PYTHONPATH: '.'
      },
    },
  ],
};
```

<sup>[Back to top ^][table-of-contents]</sup>

### 3.3. Troubleshooting

#### `ModuleNotFoundError: No module named 'simulation'`

This error is due to Python unable to locate the local Python modules. To avoid this error, ensure you have created and activate the Python virtual environment from the project root and ensure the `PYTHONPATH` is present in the `<miner|validator>.config.js` file and is pointing to the project root.

An example of a config file should be:
```js
// miner.config.js
module.exports = {
  apps: [
    {
      name: 'miner',
      script: 'python3',
      args: './neurons/miner.py --netuid 247 --logging.debug --logging.trace --subtensor.network test --wallet.name miner --wallet.hotkey default --axon.port 8091',
      env: {
        PYTHONPATH: '.'
      },
    },
  ],
};
```

As you can see, we are setting the `PYTHONPATH` environment variable that will be injected when we run `pm2 start miner`.

<sup>[Back to top ^][table-of-contents]</sup>

<!-- links -->
[table-of-contents]: #table-of-contents
