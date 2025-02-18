# Miner Guide

### Table Of Contents

* [1. Create a Wallet](#1-create-a-wallet)
* [2. Run the Miner](#2-run-the-miner)
* [3. Options](#3-options)
  - [3.1. Common Options](#31-common-options)
    - [`--axon.port INTEGER`](#--axonport-integer)
    - [`--blacklist.allow_non_registered BOOLEAN`](#--blacklistallow_non_registered-boolean)
    - [`--blacklist.force_validator_permit BOOLEAN`](#--blacklistforce_validator_permit-boolean)
    - [`--blacklist.validator_min_stake INTEGER`](#--blacklistvalidator_min_stake-integer)
    - [`--logging.debug`](#--loggingdebug)
    - [`--logging.info`](#--logginginfo)
    - [`--logging.trace`](#--loggingtrace)
    - [`--netuid INTEGER`](#--netuid-integer)
    - [`--neuron.device TEXT`](#--neurondevice-text)
    - [`--neuron.dont_save_events BOOLEAN`](#--neurondont_save_events-boolean)
    - [`--neuron.epoch_length INTEGER`](#--neuronepoch_length-integer)
    - [`--neuron.events_retention_size TEXT`](#--neuronevents_retention_size-text)
    - [`--neuron.name TEXT`](#--neuronname-text)
    - [`--neuron.timeout INTEGER`](#--neurontimeout-integer)
    - [`--neuron.vpermit_tao_limit INTEGER`](#--neuronvpermit_tao_limit-integer)
    - [`--wallet.hotkey TEXT`](#--wallethotkey-text)
    - [`--wallet.name TEXT`](#--walletname-text)
  - [3.2. Weights & Bases Options](#32-weights--bases-options)
    - [`--wandb.enabled BOOLEAN`](#--wandbenabled-boolean)
    - [`--wandb.entity TEXT`](#--wandbentity-text)
    - [`--wandb.project_name TEXT`](#--wandbenabled-boolean)
* [4. Appendix](#4-appendix)
  - [4.1. Useful Commands](#41-useful-commands)
  - [4.2. Running Multiple Miners](#42-running-multiple-miners)
  - [4.3. Troubleshooting](#43-troubleshooting)

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
  --netuid 50
```

**Step 4: Verify wallet registration (optional)**

Check the wallet has been registered:
```shell
btcli wallet overview \
  --wallet.name miner \
  --wallet.hotkey default
```

You can also check the network metagraph:
```shell
btcli subnet metagraph \
  --netuid 50
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

## 3. Options

### 3.1. Common Options

#### `--axon.port INTEGER`

The external port for the Axon component. This port is used to communicate to other neurons.

Example:

```js
// miner.config.js
module.exports = {
  apps: [
    {
      name: 'miner',
      interpreter: 'python3',
      script: './neurons/miner.py',
      args: '--axon.port 8091',
      env: {
        PYTHONPATH: '.'
      },
    },
  ],
};
```

Alternatively, you can add the args directly to the command:
```shell
pm2 start miner.config.js -- --axon.port 8091
```

<sup>[Back to top ^][table-of-contents]</sup>

#### `--blacklist.allow_non_registered BOOLEAN`

If set, miners will accept queries from non-registered entities.

> 🚨 **WARNING:** Make sure you know what you are doing when setting this option.

Default: `false`

Example:

```js
// miner.config.js
module.exports = {
  apps: [
    {
      name: 'miner',
      interpreter: 'python3',
      script: './neurons/miner.py',
      args: '--blacklist.allow_non_registered true',
      env: {
        PYTHONPATH: '.'
      },
    },
  ],
};
```

Alternatively, you can add the args directly to the command:
```shell
pm2 start miner.config.js -- --blacklist.allow_non_registered true
```

<sup>[Back to top ^][table-of-contents]</sup>

#### `--blacklist.force_validator_permit BOOLEAN`

If set, we will force incoming requests to have a permit.

Default: `false`

Example:

```js
// miner.config.js
module.exports = {
  apps: [
    {
      name: 'miner',
      interpreter: 'python3',
      script: './neurons/miner.py',
      args: '--blacklist.force_validator_permit true',
      env: {
        PYTHONPATH: '.'
      },
    },
  ],
};
```

Alternatively, you can add the args directly to the command:
```shell
pm2 start miner.config.js -- --blacklist.force_validator_permit true
```

<sup>[Back to top ^][table-of-contents]</sup>

#### `--blacklist.validator_min_stake INTEGER`

Minimum validator stake to accept forward requests from as a miner, (e.g. 1000).

Default: `1000`

Example:

```js
// miner.config.js
module.exports = {
  apps: [
    {
      name: 'miner',
      interpreter: 'python3',
      script: './neurons/miner.py',
      args: '--blacklist.validator_min_stake 1000',
      env: {
        PYTHONPATH: '.'
      },
    },
  ],
};
```

Alternatively, you can add the args directly to the command:
```shell
pm2 start miner.config.js -- --blacklist.validator_min_stake 1000
```

<sup>[Back to top ^][table-of-contents]</sup>

#### `--blacklist.validator_exceptions INTEGER INTEGER INTEGER ...`

List of validator exceptions (e.g., --blacklist.validator_exceptions 0 1 17 34 49).

Default: `[]`

Example:

```js
// miner.config.js
module.exports = {
  apps: [
    {
      name: 'miner',
      interpreter: 'python3',
      script: './neurons/miner.py',
      args: '--blacklist.validator_exceptions 0 1 17 34 49',
      env: {
        PYTHONPATH: '.'
      },
    },
  ],
};
```

Alternatively, you can add the args directly to the command:
```shell
pm2 start miner.config.js -- --blacklist.validator_exceptions 0 1 17 34 49
```

<sup>[Back to top ^][table-of-contents]</sup>

#### `--logging.debug`

Turn on bittensor debugging information.

Example:

```js
// miner.config.js
module.exports = {
  apps: [
    {
      name: 'miner',
      interpreter: 'python3',
      script: './neurons/miner.py',
      args: '--logging.debug',
      env: {
        PYTHONPATH: '.'
      },
    },
  ],
};
```

Alternatively, you can add the args directly to the command:
```shell
pm2 start miner.config.js -- --logging.debug
```

<sup>[Back to top ^][table-of-contents]</sup>

#### `--logging.info`

Turn on bittensor info level information.

Example:

```js
// miner.config.js
module.exports = {
  apps: [
    {
      name: 'miner',
      interpreter: 'python3',
      script: './neurons/miner.py',
      args: '--logging.info',
      env: {
        PYTHONPATH: '.'
      },
    },
  ],
};
```

Alternatively, you can add the args directly to the command:
```shell
pm2 start miner.config.js -- --logging.info
```

<sup>[Back to top ^][table-of-contents]</sup>

#### `--logging.trace`

Turn on bittensor trace level information.

Example:

```js
// miner.config.js
module.exports = {
  apps: [
    {
      name: 'miner',
      interpreter: 'python3',
      script: './neurons/miner.py',
      args: '--logging.trace',
      env: {
        PYTHONPATH: '.'
      },
    },
  ],
};
```

Alternatively, you can add the args directly to the command:
```shell
pm2 start miner.config.js -- --logging.trace
```

<sup>[Back to top ^][table-of-contents]</sup>

#### `--netuid INTEGER`

The netuid (network unique identifier) of the subnet within the root network, (e.g. 247).

Example:

```js
// miner.config.js
module.exports = {
  apps: [
    {
      name: 'miner',
      interpreter: 'python3',
      script: './neurons/miner.py',
      args: '--netuid 247',
      env: {
        PYTHONPATH: '.'
      },
    },
  ],
};
```

Alternatively, you can add the args directly to the command:
```shell
pm2 start miner.config.js -- --netuid 247
```

<sup>[Back to top ^][table-of-contents]</sup>

#### `--neuron.device TEXT`

The name of the device to run on.

Example:

```js
// miner.config.js
module.exports = {
  apps: [
    {
      name: 'miner',
      interpreter: 'python3',
      script: './neurons/miner.py',
      args: '--neuron.device cuda',
      env: {
        PYTHONPATH: '.'
      },
    },
  ],
};
```

Alternatively, you can add the args directly to the command:
```shell
pm2 start miner.config.js -- --neuron.device cuda
```

<sup>[Back to top ^][table-of-contents]</sup>

#### `--neuron.dont_save_events BOOLEAN`

Whether events are saved to a log file.

Default: `false`

Example:

```js
// miner.config.js
module.exports = {
  apps: [
    {
      name: 'miner',
      interpreter: 'python3',
      script: './neurons/miner.py',
      args: '--neuron.dont_save_events true',
      env: {
        PYTHONPATH: '.'
      },
    },
  ],
};
```

Alternatively, you can add the args directly to the command:
```shell
pm2 start miner.config.js -- --neuron.dont_save_events true
```

<sup>[Back to top ^][table-of-contents]</sup>

#### `--neuron.epoch_length INTEGER`

The default epoch length (how often we set weights, measured in 12 second blocks), (e.g. 100).

Example:

```js
// miner.config.js
module.exports = {
  apps: [
    {
      name: 'miner',
      interpreter: 'python3',
      script: './neurons/miner.py',
      args: '--neuron.epoch_length 100',
      env: {
        PYTHONPATH: '.'
      },
    },
  ],
};
```

Alternatively, you can add the args directly to the command:
```shell
pm2 start miner.config.js -- --neuron.epoch_length 100
```

<sup>[Back to top ^][table-of-contents]</sup>

#### `--neuron.events_retention_size TEXT`

The events retention size.

Default: `2147483648` (2GB)

Example:

```js
// miner.config.js
module.exports = {
  apps: [
    {
      name: 'miner',
      interpreter: 'python3',
      script: './neurons/miner.py',
      args: '--neuron.events_retention_size 2147483648',
      env: {
        PYTHONPATH: '.'
      },
    },
  ],
};
```

Alternatively, you can add the args directly to the command:
```shell
pm2 start miner.config.js -- --neuron.events_retention_size 2147483648
```

<sup>[Back to top ^][table-of-contents]</sup>

#### `--neuron.name TEXT`

Trials for this neuron go in neuron.root / (wallet_cold - wallet_hot) / neuron.name.

Default: `miner`

Example:

```js
// miner.config.js
module.exports = {
  apps: [
    {
      name: 'miner',
      interpreter: 'python3',
      script: './neurons/miner.py',
      args: '--neuron.name miner',
      env: {
        PYTHONPATH: '.'
      },
    },
  ],
};
```

Alternatively, you can add the args directly to the command:
```shell
pm2 start miner.config.js -- --neuron.name miner
```

<sup>[Back to top ^][table-of-contents]</sup>

#### `--neuron.timeout INTEGER`

The maximum timeout in seconds of the miner neuron response, (e.g. 120).

Default: `-`

Example:

```js
// miner.config.js
module.exports = {
  apps: [
    {
      name: 'miner',
      interpreter: 'python3',
      script: './neurons/miner.py',
      args: '--neuron.timeout 120',
      env: {
        PYTHONPATH: '.'
      },
    },
  ],
};
```

Alternatively, you can add the args directly to the command:
```shell
pm2 start miner.config.js -- --neuron.timeout 120
```

<sup>[Back to top ^][table-of-contents]</sup>

#### `--neuron.vpermit_tao_limit INTEGER`

The maximum number of TAO allowed that is allowed for the validator to process miner response, (e.g. 1000).

Default: `4096`

Example:

```js
// miner.config.js
module.exports = {
  apps: [
    {
      name: 'miner',
      interpreter: 'python3',
      script: './neurons/miner.py',
      args: '--neuron.vpermit_tao_limit 1000',
      env: {
        PYTHONPATH: '.'
      },
    },
  ],
};
```

Alternatively, you can add the args directly to the command:
```shell
pm2 start miner.config.js -- --neuron.vpermit_tao_limit 1000
```

<sup>[Back to top ^][table-of-contents]</sup>

#### `--wallet.hotkey TEXT`

The hotkey of the wallet.

Example:

```js
// miner.config.js
module.exports = {
  apps: [
    {
      name: 'miner',
      interpreter: 'python3',
      script: './neurons/miner.py',
      args: '--wallet.hotkey default',
      env: {
        PYTHONPATH: '.'
      },
    },
  ],
};
```

Alternatively, you can add the args directly to the command:
```shell
pm2 start miner.config.js -- --wallet.hotkey default
```

<sup>[Back to top ^][table-of-contents]</sup>

#### `--wallet.name TEXT`

The name of the wallet to use.

Example:

```js
// miner.config.js
module.exports = {
  apps: [
    {
      name: 'miner',
      interpreter: 'python3',
      script: './neurons/miner.py',
      args: '--wallet.name miner',
      env: {
        PYTHONPATH: '.'
      },
    },
  ],
};
```

Alternatively, you can add the args directly to the command:
```shell
pm2 start miner.config.js -- --wallet.name miner
```

<sup>[Back to top ^][table-of-contents]</sup>

### 3.2. Weights & Bases Options

#### `--wandb.enabled BOOLEAN`

Boolean toggle for wandb integration.

Default: `false`

Example:

```js
// miner.config.js
module.exports = {
  apps: [
    {
      name: 'miner',
      interpreter: 'python3',
      script: './neurons/miner.py',
      args: '--wandb.enabled true',
      env: {
        PYTHONPATH: '.'
      },
    },
  ],
};
```

Alternatively, you can add the args directly to the command:
```shell
pm2 start miner.config.js -- --wandb.enabled true
```

<sup>[Back to top ^][table-of-contents]</sup>

#### `--wandb.entity TEXT`

The username or team name where you want to send W&B runs.

Default: `opentensor-dev`

Example:

```js
// miner.config.js
module.exports = {
  apps: [
    {
      name: 'miner',
      interpreter: 'python3',
      script: './neurons/miner.py',
      args: '--wandb.entity opentensor-dev',
      env: {
        PYTHONPATH: '.'
      },
    },
  ],
};
```

Alternatively, you can add the args directly to the command:
```shell
pm2 start miner.config.js -- --wandb.entity opentensor-dev
```

<sup>[Back to top ^][table-of-contents]</sup>

#### `--wandb.project_name TEXT`

The name of the project where W&B runs.

Default: `template-miners`

Example:

```js
// miner.config.js
module.exports = {
  apps: [
    {
      name: 'miner',
      interpreter: 'python3',
      script: './neurons/miner.py',
      args: '--wandb.project_name template-miners',
      env: {
        PYTHONPATH: '.'
      },
    },
  ],
};
```

Alternatively, you can add the args directly to the command:
```shell
pm2 start miner.config.js -- --wandb.project_name template-miners
```

<sup>[Back to top ^][table-of-contents]</sup>

## 4. Appendix

### 4.1. Useful Commands

| Command                      | Description                 |
|------------------------------|-----------------------------|
| `pm2 stop miner`             | Stops the miner.            |
| `pm2 logs miner --lines 100` | View the logs of the miner. |

<sup>[Back to top ^][table-of-contents]</sup>

### 4.2. Running Multiple Miners

In order to run multiple miners on the same machine, you must ensure that you correctly edit the `miner.config.js` file to allow for multiple apps. 

Each app **MUST** use a separate wallet (see [here](#1-create-a-wallet) for instructions on how to create a wallet) and it **MUST** use a different port for the `--axon.port` parameter in the start-up script.

An example for a config using multiple miners:

```js
// miner.config.js
module.exports = {
  apps: [
    {
      name: 'miner-1',
      interpreter: 'python3',
      script: './neurons/miner.py',
      args: '--netuid 247 --logging.debug --logging.trace --subtensor.network test --wallet.name miner_1 --wallet.hotkey default --axon.port 8091',
      env: {
        PYTHONPATH: '.'
      },
    },
    {
      name: 'miner-2',
      interpreter: 'python3',
      script: './neurons/miner.py',
      args: '--netuid 247 --logging.debug --logging.trace --subtensor.network test --wallet.name miner_2 --wallet.hotkey default --axon.port 8092',
      env: {
        PYTHONPATH: '.'
      },
    },
  ],
};
```

<sup>[Back to top ^][table-of-contents]</sup>

### 4.3. Troubleshooting

#### `ModuleNotFoundError: No module named 'simulation'`

This error is due to Python unable to locate the local Python modules. To avoid this error, ensure you have created and activate the Python virtual environment from the project root and ensure the `PYTHONPATH` is present in the `<miner|validator>.config.js` file and is pointing to the project root.

An example of a config file should be:
```js
// miner.config.js
module.exports = {
  apps: [
    {
      name: 'miner',
      interpreter: 'python3',
      script: './neurons/miner.py',
      args: '--netuid 247 --logging.debug --logging.trace --subtensor.network test --wallet.name miner --wallet.hotkey default --axon.port 8091',
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
