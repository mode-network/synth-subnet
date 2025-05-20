# Validator

### Table Of Contents

* [1. Create a Wallet](#1-create-a-wallet)
* [2. Run the Validator](#2-run-the-validator)
* [3. Options](#3-options)
  - [3.1. Common Options](#31-common-options)
    - [`--axon.port INTEGER`](#--axonport-integer)
    - [`--ewma.cutoff_days INTEGER`](#--ewmacutoff_days-integer)
    - [`--ewma.half_life_days FLOAT`](#--ewmahalf_life_days-float)
    - [`--logging.debug`](#--loggingdebug)
    - [`--logging.info`](#--logginginfo)
    - [`--logging.trace`](#--loggingtrace)
    - [`--netuid INTEGER`](#--netuid-integer)
    - [`--neuron.axon_off BOOLEAN`](#--neuronaxon_off-boolean)
    - [`--neuron.device TEXT`](#--neurondevice-text)
    - [`--neuron.disable_set_weights BOOLEAN`](#--neurondisable_set_weights-boolean)
    - [`--neuron.dont_save_events BOOLEAN`](#--neurondont_save_events-boolean)
    - [`--neuron.epoch_length INTEGER`](#--neuronepoch_length-integer)
    - [`--neuron.events_retention_size TEXT`](#--neuronevents_retention_size-text)
    - [`--neuron.name TEXT`](#--neuronname-text)
    - [`--neuron.sample_size INTEGER`](#--neuronsample_size-integer)
    - [`--neuron.timeout INTEGER`](#--neurontimeout-integer)
    - [`--neuron.vpermit_tao_limit INTEGER`](#--neuronvpermit_tao_limit-integer)
    - [`--wallet.hotkey TEXT`](#--wallethotkey-text)
    - [`--wallet.name TEXT`](#--walletname-text)
  - [3.2. Logging Options](#32-logging-options)
    - [`--gcp.log_id_prefix TEXT`](#--gcplog_id_prefix-text)
* [4. Appendix](#4-appendix)
  - [4.1. Useful Commands](#41-useful-commands)

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
  --wallet.name validator \
  --wallet.hotkey default
```

> 🚨 **WARNING:** You must ensure your wallets have enough TAO (0.1 should be sufficient) to be start mining. For testnet, you can use the [`btcli wallet faucet`](https://docs.bittensor.com/btcli#btcli-wallet-faucet).

**Step 3: Register wallet**

Acquire a slot on the Bittensor subnet by registering the wallet:
```shell
btcli subnet register \
  --wallet.name validator \
  --wallet.hotkey default \
  --netuid 50
```

```shell
btcli root register --wallet.name validator --wallet.hotkey default
```

4. Stake:
```shell
btcli stake add \
  --wallet.name validator \
  --wallet.hotkey default \
  --netuid 50
```

**Step 4: Verify wallet registration (optional)**

Check the wallet has been registered:
```shell
btcli wallet overview \
  --wallet.name validator \
  --wallet.hotkey default
```

You can also check the network metagraph:
```shell
btcli subnet metagraph \
  --netuid 50
```

<sup>[Back to top ^][table-of-contents]</sup>

## 2. Run the Validator

**Step 1: Database setup**
- Create a postgres database with the name "synth"
- Rename the ".env.example" in the root of the repo to ".env"
- Update the DB_URL in ".env" file to correct postgres server IP, username and password

**Step 2: Activate the Python virtual environment**

```shell
source bt_venv/bin/activate
```

**Step 3: Run database migrations**
```shell
alembic upgrade head
```

**Step 4: Start PM2 with the validator config**

```shell
pm2 start validator.config.js
```

**Step 5: Check the validator is running (optional)**

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
// validator.config.js
module.exports = {
  apps: [
    {
      name: 'validator',
      interpreter: 'python3',
      script: './neurons/validator.py',
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
pm2 start validator.config.js -- --axon.port 8091
```

<sup>[Back to top ^][table-of-contents]</sup>

#### `--ewma.cutoff_days INTEGER`

The number of days against which to run the moving average, (e.g. 1).

Default: `2`

Example:

```js
// validator.config.js
module.exports = {
  apps: [
    {
      name: 'validator',
      interpreter: 'python3',
      script: './neurons/validator.py',
      args: '--ewma.cutoff_days 1',
      env: {
        PYTHONPATH: '.'
      },
    },
  ],
};
```

Alternatively, you can add the args directly to the command:
```shell
pm2 start validator.config.js -- --ewma.cutoff_days 1
```

<sup>[Back to top ^][table-of-contents]</sup>

#### `--ewma.half_life_days FLOAT`

The half-life in days for the exponential decay, (e.g. 2.0).

Default: `1.0`

Example:

```js
// validator.config.js
module.exports = {
  apps: [
    {
      name: 'validator',
      interpreter: 'python3',
      script: './neurons/validator.py',
      args: '--ewma.half_life_days 2.0',
      env: {
        PYTHONPATH: '.'
      },
    },
  ],
};
```

Alternatively, you can add the args directly to the command:
```shell
pm2 start validator.config.js -- --ewma.half_life_days 2.0
```

<sup>[Back to top ^][table-of-contents]</sup>

#### `--softmax.beta FLOAT`

Negative beta to give higher weight to lower scores.

Default: `-0.002`

Example:

```js
// validator.config.js
module.exports = {
  apps: [
    {
      name: 'validator',
      interpreter: 'python3',
      script: './neurons/validator.py',
      args: '--softmax.beta -0.003',
      env: {
        PYTHONPATH: '.'
      },
    },
  ],
};
```

Alternatively, you can add the args directly to the command:
```shell
pm2 start validator.config.js -- --softmax.beta -0.003
```

<sup>[Back to top ^][table-of-contents]</sup>

#### `--logging.debug`

Turn on bittensor debugging information.

Example:

```js
// validator.config.js
module.exports = {
  apps: [
    {
      name: 'validator',
      interpreter: 'python3',
      script: './neurons/validator.py',
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
pm2 start validator.config.js -- --logging.debug
```

<sup>[Back to top ^][table-of-contents]</sup>

#### `--logging.info`

Turn on bittensor info level information.

Example:

```js
// validator.config.js
module.exports = {
  apps: [
    {
      name: 'validator',
      interpreter: 'python3',
      script: './neurons/validator.py',
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
pm2 start validator.config.js -- --logging.info
```

<sup>[Back to top ^][table-of-contents]</sup>

#### `--logging.trace`

Turn on bittensor trace level information.

Example:

```js
// validator.config.js
module.exports = {
  apps: [
    {
      name: 'validator',
      interpreter: 'python3',
      script: './neurons/validator.py',
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
pm2 start validator.config.js -- --logging.trace
```

<sup>[Back to top ^][table-of-contents]</sup>

#### `--netuid INTEGER`

The netuid (network unique identifier) of the subnet within the root network, (e.g. 247).

Example:

```js
// validator.config.js
module.exports = {
  apps: [
    {
      name: 'validator',
      interpreter: 'python3',
      script: './neurons/validator.py',
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
pm2 start validator.config.js -- --netuid 247
```

<sup>[Back to top ^][table-of-contents]</sup>

#### `--neuron.axon_off BOOLEAN`

This will switch off the Axon component.

Default: `false`

Example:

```js
// validator.config.js
module.exports = {
  apps: [
    {
      name: 'validator',
      interpreter: 'python3',
      script: './neurons/validator.py',
      args: '--neuron.axon_off true',
      env: {
        PYTHONPATH: '.'
      },
    },
  ],
};
```

Alternatively, you can add the args directly to the command:
```shell
pm2 start validator.config.js -- --neuron.axon_off true
```

<sup>[Back to top ^][table-of-contents]</sup>

#### `--neuron.device TEXT`

The name of the device to run on.

Example:

```js
// validator.config.js
module.exports = {
  apps: [
    {
      name: 'validator',
      interpreter: 'python3',
      script: './neurons/validator.py',
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
pm2 start validator.config.js -- --neuron.device cuda
```

<sup>[Back to top ^][table-of-contents]</sup>

#### `--neuron.dont_save_events BOOLEAN`

Whether events are saved to a log file.

Default: `false`

Example:

```js
// validator.config.js
module.exports = {
  apps: [
    {
      name: 'validator',
      interpreter: 'python3',
      script: './neurons/validator.py',
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
pm2 start validator.config.js -- --neuron.dont_save_events true
```

<sup>[Back to top ^][table-of-contents]</sup>

#### `--neuron.disable_set_weights BOOLEAN`

Disables setting weights.

Default: `false`

Example:

```js
// validator.config.js
module.exports = {
  apps: [
    {
      name: 'validator',
      interpreter: 'python3',
      script: './neurons/validator.py',
      args: '--neuron.disable_set_weights true',
      env: {
        PYTHONPATH: '.'
      },
    },
  ],
};
```

Alternatively, you can add the args directly to the command:
```shell
pm2 start validator.config.js -- --neuron.disable_set_weights true
```

<sup>[Back to top ^][table-of-contents]</sup>

#### `--neuron.epoch_length INTEGER`

The default epoch length (how often we set weights, measured in 12 second blocks), (e.g. 100).

Example:

```js
// validator.config.js
module.exports = {
  apps: [
    {
      name: 'validator',
      interpreter: 'python3',
      script: './neurons/validator.py',
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
pm2 start validator.config.js -- --neuron.epoch_length 100
```

<sup>[Back to top ^][table-of-contents]</sup>

#### `--neuron.events_retention_size TEXT`

The events retention size.

Default: `2147483648` (2GB)

Example:

```js
// validator.config.js
module.exports = {
  apps: [
    {
      name: 'validator',
      interpreter: 'python3',
      script: './neurons/validator.py',
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
pm2 start validator.config.js -- --neuron.events_retention_size 2147483648
```

<sup>[Back to top ^][table-of-contents]</sup>

#### `--neuron.name TEXT`

Trials for this neuron go in neuron.root / (wallet_cold - wallet_hot) / neuron.name.

Default: `validator`

Example:

```js
// validator.config.js
module.exports = {
  apps: [
    {
      name: 'validator',
      interpreter: 'python3',
      script: './neurons/validator.py',
      args: '--neuron.name validator',
      env: {
        PYTHONPATH: '.'
      },
    },
  ],
};
```

Alternatively, you can add the args directly to the command:
```shell
pm2 start validator.config.js -- --neuron.name validator
```

<sup>[Back to top ^][table-of-contents]</sup>

#### `--neuron.sample_size INTEGER`

The number of validators to query in a single step.

Default: `50`

Example:

```js
// validator.config.js
module.exports = {
  apps: [
    {
      name: 'validator',
      interpreter: 'python3',
      script: './neurons/validator.py',
      args: '--neuron.sample_size 50',
      env: {
        PYTHONPATH: '.'
      },
    },
  ],
};
```

Alternatively, you can add the args directly to the command:
```shell
pm2 start validator.config.js -- --neuron.sample_size 50
```

<sup>[Back to top ^][table-of-contents]</sup>

#### `--neuron.timeout INTEGER`

The maximum timeout in seconds of the validator neuron response, (e.g. 120).

Default: `-`

Example:

```js
// validator.config.js
module.exports = {
  apps: [
    {
      name: 'validator',
      interpreter: 'python3',
      script: './neurons/validator.py',
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
pm2 start validator.config.js -- --neuron.timeout 120
```

<sup>[Back to top ^][table-of-contents]</sup>

#### `--neuron.vpermit_tao_limit INTEGER`

The maximum number of TAO allowed that is allowed for the validator to process validator response, (e.g. 1000).

Default: `4096`

Example:

```js
// validator.config.js
module.exports = {
  apps: [
    {
      name: 'validator',
      interpreter: 'python3',
      script: './neurons/validator.py',
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
pm2 start validator.config.js -- --neuron.vpermit_tao_limit 1000
```

<sup>[Back to top ^][table-of-contents]</sup>

#### `--wallet.hotkey TEXT`

The hotkey of the wallet.

Example:

```js
// validator.config.js
module.exports = {
  apps: [
    {
      name: 'validator',
      interpreter: 'python3',
      script: './neurons/validator.py',
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
pm2 start validator.config.js -- --wallet.hotkey default
```

<sup>[Back to top ^][table-of-contents]</sup>

#### `--wallet.name TEXT`

The name of the wallet to use.

Example:

```js
// validator.config.js
module.exports = {
  apps: [
    {
      name: 'validator',
      interpreter: 'python3',
      script: './neurons/validator.py',
      args: '--wallet.name validator',
      env: {
        PYTHONPATH: '.'
      },
    },
  ],
};
```

Alternatively, you can add the args directly to the command:
```shell
pm2 start validator.config.js -- --wallet.name validator
```

<sup>[Back to top ^][table-of-contents]</sup>

### 3.2. Logging Options

#### `--gcp.log_id_prefix TEXT`

String to set the GCP log ID prefix.

Example:

```js
// validator.config.js
module.exports = {
  apps: [
    {
      name: 'validator',
      interpreter: 'python3',
      script: './neurons/validator.py',
      args: '--gcp.log_id_prefix my_validator_name',
      env: {
        PYTHONPATH: '.'
      },
    },
  ],
};
```

Alternatively, you can add the args directly to the command:
```shell
pm2 start validator.config.js -- --gcp.log_id_prefix my_validator_name
```

<sup>[Back to top ^][table-of-contents]</sup>

## 4. Appendix

### 4.1. Useful Commands

| Command                          | Description                     |
|----------------------------------|---------------------------------|
| `pm2 stop validator`             | Stops the validator.            |
| `pm2 logs validator --lines 100` | View the logs of the validator. |

<sup>[Back to top ^][table-of-contents]</sup>

<!-- links -->
[table-of-contents]: #table-of-contents
