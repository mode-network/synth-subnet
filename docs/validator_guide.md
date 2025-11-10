# Validator

### Table Of Contents

- [1. Requirements](#1-requirements)
- [2. Setup](#2-setup)
- [3. Create a Wallet](#3-create-a-wallet)
- [4. Run the Validator](#4-run-the-validator)
- [5. Options](#5-options)
  - [5.1. Common Options](#51-common-options)
    - [`--axon.port INTEGER`](#--axonport-integer)
    - [`--ewma.cutoff_days INTEGER`](#--ewmacutoff_days-integer)
    - [`--ewma.window_days INTEGER`](#--ewmawindow_days-float)
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
    - [`--neuron.nprocs INTEGER`](#--neuronnprocs-integer)
    - [`--neuron.use_multiprocess INTEGER`](#--neuronuse_multiprocess-integer)
    - [`--neuron.vpermit_tao_limit INTEGER`](#--neuronvpermit_tao_limit-integer)
    - [`--wallet.hotkey TEXT`](#--wallethotkey-text)
    - [`--wallet.name TEXT`](#--walletname-text)
  - [5.2. Logging Options](#52-logging-options)
    - [`--gcp.log_id_prefix TEXT`](#--gcplog_id_prefix-text)
- [6. Appendix](#4-appendix)
  - [6.1. Useful Commands](#41-useful-commands)

### 1. Requirements

- [Git](https://github.com/git-guides/install-git)
- [Ubuntu v20.04+](https://ubuntu.com/download)

<sup>[Back to top ^][table-of-contents]</sup>

### 2. Setup

**Step 1: Clone the repository**

```shell
git clone https://github.com/mode-network/synth-subnet.git
```

**Step 2: Change directory to the project root**

```shell
cd ./synth-subnet
```

**Step 3: Add the required repositories**

```shell
sudo add-apt-repository ppa:deadsnakes/ppa
```

> ⚠️ **NOTE:** The [deadsnakes](https://github.com/deadsnakes) repository, while unofficial, it is hugely popular and used by many Python projects.

**Step 4: Install Rust**

```shell
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

**Step 5: Install the dependencies**

```shell
sudo apt update && \
  sudo apt install nodejs npm python3.10 pkg-config
```

**Step 6: Install [PM2](https://pm2.io/)**

```shell
sudo npm install pm2 -g
```

**Step 7: Install the Python environment**

```shell
sudo apt install python3.10-venv
```

**Step 8: Create a new Python environment**

```shell
python3.10 -m venv bt_venv
```

**Step 9: Activate and switch to the newly created Python virtual environment**

```shell
source bt_venv/bin/activate
```

This should activate the `bt_venv` environment and you should see the command line prefixed with `(bt_venv)`.

**Step 10: Install local Python dependencies**

With the Python virtual environment active, install the Python dependencies:

```shell
pip install -r requirements.txt
```

## 3. Create a Wallet

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

if you want to try it on testnet first, run the following command:

```shell
btcli subnet register \
  --wallet.name validator \
  --wallet.hotkey default \
  --network test \
  --netuid 247
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

for testnet:

```shell
btcli stake add \
  --wallet.name validator \
  --wallet.hotkey default \
  --network test \
  --netuid 247
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

for testnet it's:

```shell
btcli subnet metagraph \
  --network test \
  --netuid 247
```

<sup>[Back to top ^][table-of-contents]</sup>

## 4. Run the Validator

**Step 1: Database setup**

- Create a postgres database with the name "synth"
- Rename the ".env.example" in the root of the repo to ".env"
- Update the `.env` file with your database credentials.

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

for testnet use:

```shell
pm2 start validator.test.config.js
```

**Step 5: Check the validator is running (optional)**

```shell
pm2 list
```

<sup>[Back to top ^][table-of-contents]</sup>

## 5. Options

### 5.1. Common Options

#### `--axon.port INTEGER`

The external port for the Axon component. This port is used to communicate to other neurons.

Example:

```js
// validator.config.js
module.exports = {
  apps: [
    {
      name: "validator",
      interpreter: "python3",
      script: "./neurons/validator.py",
      args: "--axon.port 8091",
      env: {
        PYTHONPATH: ".",
      },
    },
  ],
};
```

Alternatively, you can add the args directly to the command:

```shell
pm2 start validator.config.js -- --axon.port 8091
```

for testnet it's:

```shell
pm2 start validator.test.config.js -- --axon.port 8091
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
      name: "validator",
      interpreter: "python3",
      script: "./neurons/validator.py",
      args: "--ewma.cutoff_days 10",
      env: {
        PYTHONPATH: ".",
      },
    },
  ],
};
```

Alternatively, you can add the args directly to the command:

```shell
pm2 start validator.config.js -- --ewma.cutoff_days 10
```

for testnet it's:

```shell
pm2 start validator.test.config.js -- --ewma.cutoff_days 10
```

<sup>[Back to top ^][table-of-contents]</sup>

#### `--ewma.window_days INTEGER`

The window in days for the rolling average, (e.g. 10).

Default: `10`

Example:

```js
// validator.config.js
module.exports = {
  apps: [
    {
      name: "validator",
      interpreter: "python3",
      script: "./neurons/validator.py",
      args: "--ewma.window_days 10",
      env: {
        PYTHONPATH: ".",
      },
    },
  ],
};
```

Alternatively, you can add the args directly to the command:

```shell
pm2 start validator.config.js -- --ewma.window_days 10
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
      name: "validator",
      interpreter: "python3",
      script: "./neurons/validator.py",
      args: "--softmax.beta -0.003",
      env: {
        PYTHONPATH: ".",
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
      name: "validator",
      interpreter: "python3",
      script: "./neurons/validator.py",
      args: "--logging.debug",
      env: {
        PYTHONPATH: ".",
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
      name: "validator",
      interpreter: "python3",
      script: "./neurons/validator.py",
      args: "--logging.info",
      env: {
        PYTHONPATH: ".",
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
      name: "validator",
      interpreter: "python3",
      script: "./neurons/validator.py",
      args: "--logging.trace",
      env: {
        PYTHONPATH: ".",
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
      name: "validator",
      interpreter: "python3",
      script: "./neurons/validator.py",
      args: "--netuid 247",
      env: {
        PYTHONPATH: ".",
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
      name: "validator",
      interpreter: "python3",
      script: "./neurons/validator.py",
      args: "--neuron.axon_off true",
      env: {
        PYTHONPATH: ".",
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
      name: "validator",
      interpreter: "python3",
      script: "./neurons/validator.py",
      args: "--neuron.device cuda",
      env: {
        PYTHONPATH: ".",
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
      name: "validator",
      interpreter: "python3",
      script: "./neurons/validator.py",
      args: "--neuron.dont_save_events true",
      env: {
        PYTHONPATH: ".",
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
      name: "validator",
      interpreter: "python3",
      script: "./neurons/validator.py",
      args: "--neuron.disable_set_weights true",
      env: {
        PYTHONPATH: ".",
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
      name: "validator",
      interpreter: "python3",
      script: "./neurons/validator.py",
      args: "--neuron.epoch_length 100",
      env: {
        PYTHONPATH: ".",
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
      name: "validator",
      interpreter: "python3",
      script: "./neurons/validator.py",
      args: "--neuron.events_retention_size 2147483648",
      env: {
        PYTHONPATH: ".",
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
      name: "validator",
      interpreter: "python3",
      script: "./neurons/validator.py",
      args: "--neuron.name validator",
      env: {
        PYTHONPATH: ".",
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
      name: "validator",
      interpreter: "python3",
      script: "./neurons/validator.py",
      args: "--neuron.sample_size 50",
      env: {
        PYTHONPATH: ".",
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
      name: "validator",
      interpreter: "python3",
      script: "./neurons/validator.py",
      args: "--neuron.timeout 120",
      env: {
        PYTHONPATH: ".",
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

#### `--neuron.nprocs INTEGER`

The number of processes to run for the validator dendrite, (e.g. 8).

Default: `8`

Example:

```js
// validator.config.js
module.exports = {
  apps: [
    {
      name: "validator",
      interpreter: "python3",
      script: "./neurons/validator.py",
      args: "--neuron.nprocs 8",
      env: {
        PYTHONPATH: ".",
      },
    },
  ],
};
```

Alternatively, you can add the args directly to the command:

```shell
pm2 start validator.config.js -- --neuron.nprocs 8
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
      name: "validator",
      interpreter: "python3",
      script: "./neurons/validator.py",
      args: "--neuron.vpermit_tao_limit 1000",
      env: {
        PYTHONPATH: ".",
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

#### `--neuron.use_multiprocess INTEGER`

Wether to use multiple processes for the validator dendrite.

Default: `1`

Example to disable multiprocess:

```js
// validator.config.js
module.exports = {
  apps: [
    {
      name: "validator",
      interpreter: "python3",
      script: "./neurons/validator.py",
      args: "--neuron.use_multiprocess 0",
      env: {
        PYTHONPATH: ".",
      },
    },
  ],
};
```

Alternatively, you can add the args directly to the command:

```shell
pm2 start validator.config.js -- --neuron.nprocs 8
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
      name: "validator",
      interpreter: "python3",
      script: "./neurons/validator.py",
      args: "--wallet.hotkey default",
      env: {
        PYTHONPATH: ".",
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
      name: "validator",
      interpreter: "python3",
      script: "./neurons/validator.py",
      args: "--wallet.name validator",
      env: {
        PYTHONPATH: ".",
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

### 5.2. Logging Options

#### `--gcp.log_id_prefix TEXT`

String to set the GCP log ID prefix.

Example:

```js
// validator.config.js
module.exports = {
  apps: [
    {
      name: "validator",
      interpreter: "python3",
      script: "./neurons/validator.py",
      args: "--gcp.log_id_prefix my_validator_name",
      env: {
        PYTHONPATH: ".",
      },
    },
  ],
};
```

Alternatively, you can add the args directly to the command:

```shell
pm2 start validator.config.js -- --gcp.log_id_prefix my_validator_name
```

for testnet it's:

```shell
pm2 start validator.test.config.js -- --gcp.log_id_prefix my_validator_name
```

<sup>[Back to top ^][table-of-contents]</sup>

## 6. Appendix

### 6.1. Useful Commands

| Command                          | Description                     |
| -------------------------------- | ------------------------------- |
| `pm2 stop validator`             | Stops the validator.            |
| `pm2 logs validator --lines 100` | View the logs of the validator. |

<sup>[Back to top ^][table-of-contents]</sup>

<!-- links -->

[table-of-contents]: #table-of-contents

## Run the validator with Docker

You can run the validator with Docker and docker compose.

1. Run this command:

```
cp .env.validator .env.validator.local
```

2. Optionally edit "POSTGRES_PASSWORD" in `.env.validator.local` file. Don't change "POSTGRES_DB"
3. Optionally edit `entrypoint-validator.sh` file, especially `log_id_prefix` if you want to forward the log to a GCP Log Bucket. For this you need the credential file and environment variable GOOGLE_APPLICATION_CREDENTIALS set to the path of this file
4. Install docker

```
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
sudo groupadd docker
sudo usermod -aG docker $USER
newgrp docker
```

5. Setup your bittensor wallet
6. Start: `docker compose up --build --remove-orphans --force-recreate -d`
7. Update with the same command
8. Read the logs: `docker compose logs -f`

### Useful commands

- Enter into the postgres container: `docker exec -it postgres bash`
- Enter into psql: `psql -U postgres`
- List databases: `\l`. You should see "synth" database
- Connect to database: `\c synth`

### Useful SQL queries

- Check the validator requests: `select * from validator_requests order by start_time desc limit 500;`
- Check the miner predictions:

```
select mp.format_validation, vr.start_time, miners.miner_uid, process_time
from miner_predictions mp
join validator_requests vr on mp.validator_requests_id = vr.id
join miners on miners.id = mp.miner_id
order by start_time desc
limit 500;
```

- Check the miner scores:

```
SELECT
	MS.ID,
	MP.VALIDATOR_REQUESTS_ID,
	MS.MINER_PREDICTIONS_ID,
	VR.START_TIME,
	VR.ASSET,
	MS.SCORED_TIME,
	MINERS.MINER_UID,
	(MS.SCORE_DETAILS_V3 -> 'total_crps')::FLOAT AS CSRP,
	MS.PROMPT_SCORE_V3
FROM
	MINER_SCORES MS
	JOIN MINER_PREDICTIONS MP ON MP.ID = MS.MINER_PREDICTIONS_ID
	JOIN MINERS ON MINERS.ID = MP.MINER_ID
	JOIN VALIDATOR_REQUESTS VR ON VR.ID = MP.VALIDATOR_REQUESTS_ID
WHERE
	MS.SCORED_TIME > NOW()::DATE - 10
ORDER BY
	MS.ID DESC;
```

- Check the predictions table size: `SELECT pg_size_pretty(pg_total_relation_size('miner_predictions')) as size;`
- /!\ Delete old predictions

```
delete from miner_predictions
using validator_requests
where miner_predictions.validator_requests_id = validator_requests.id
and validator_requests.start_time < now()::DATE - 20;
```
