# Miner Reference

### Table of contents

* [1. Options](#1-options)
  - [1.1. Common Options](#11-common-options)
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
  - [1.2. Weights & Bases Options](#12-weights--bases-options)
    - [`--wandb.enabled BOOLEAN`](#--wandbenabled-boolean)
    - [`--wandb.entity TEXT`](#--wandbentity-text)
    - [`--wandb.project_name TEXT`](#--wandbenabled-boolean)
* [2. Appendix](#2-appendix)
  - [2.1. FAQs](#21-faqs)
  - [2.2. Useful Commands](#22-useful-commands)
  - [2.3. Troubleshooting](#23-troubleshooting)

## 1. Options

### 1.1. Common Options

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

### 1.2. Weights & Bases Options

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

## 2. Appendix

### 2.1. FAQs

#### 1. What is the best way for me to track my miner’s performance?

Visit the Synth Miner Dashboard: [https://miners.synthdata.co/](https://miners.synthdata.co/).

#### 2. How long do I have to wait for my new miner to get the first CRPS score?

Assuming your setup is correct, and you're submitting predictions, it takes approximately 25–27 hours for your first score to appear. This is because the CRPS is calculated based on the actual price 24 hours after the prediction.

#### 3. What does it mean when I check [https://synth.mode.network/validation/miner?uid=](https://synth.mode.network/validation/miner?uid=) and see: `{"validated":false,"reason":"Number of time points is incorrect: expected 289, got 288","response_time":"4.07"}`?

It means your submission has the wrong number of time points. You should be submitting exactly 289 points for that prompt.

#### 4. Why does my miner keep getting a CRPS score of -1?

Your miner will receive a CRPS score of -1 if the prediction is not in the correct format.

#### 5. What is the correct format for subnet-prompted predictions?

A prediction is considered valid if it meets all the following conditions:
* Submitted before the timeout specified by the validator.
* Contains the required number of simulation paths (num_simulations in the prompt).
* Each path has the correct number of time points:
```python
expected_time_points = (time_length / time_increment) + 1
```
* The first point of all paths matches the prompt’s start_time.
* All timestamps are in ISO format, e.g., 2025-07-15T20:59:00+00:00.
* Time intervals between points match time_increment exactly (in seconds).
* All price values are of type int or float.

You can find the validation function here:

[response_validation.py#L35](https://github.com/mode-network/synth-subnet/blob/d694aa0bff3742498296809699c86e45b037289c/synth/validator/response_validation.py#L35)

And an example of the prompt parameters here:

[validator.py#L71](https://github.com/mode-network/synth-subnet/blob/44921d343e6f8ba770558018a28508796ce2a3ce/neurons/validator.py#L71)

#### 6. Synth is predicting multiple assets. How does each asset prediction contribute to the smoothed score?

Each asset—BTC, ETH, XAU and SOL—contributes equally to the smoothed score. This means your miner must perform consistently well across all four. Future assets added to Synth will have the same weighting, unless noted otherwise.

#### 7. I am getting fair CRPS scores but my reward weight is still zero. Why?

Miner reward weights are determined by a softmax function over a smoothed average of your CRPS scores over the past 10 days, using a 5-day half-life. This is subject to future change.

#### 8. How do I improve my miner performance?

Synth regularly publishes detailed miner performance reviews highlighting the strategies of top-performing miners. These reviews can help you optimize your own models.

Explore them here: [https://mirror.xyz/synthdata.eth](https://mirror.xyz/synthdata.eth)

#### 9. Do I need to run multiple miners?

If you're using the same model per asset, no—rewards will be the same. If you're using different models, then yes, it's encouraged to experiment and find what works best.

#### 10. Is there a penalty for turning off my miner for a period of time?

If you miss a prompt or submit in an incorrect format, your prompt score will be penalized—set to the 90th percentile of all prompt scores, and your CRPS will be -1.

If you make the best prediction, your prompt score will be 0, and your CRPS will also be at its minimum of 0, meaning your prediction perfectly matched the realized price.

The smoothed score is calculated as the moving average of your prompt scores over time.

These rules apply across all assets.

#### 11. Could someone explain how trust values are calculated?

Trust is computed based on the subnet's scoring system, outlined here: [https://docs.bittensor.com/emissions#trust](https://docs.bittensor.com/emissions#trust)

It aggregates individual miner scores into a consensus-based weighting system, which determines reward distribution based on prediction quality.

#### 12. How much time do miners have to respond to a prompt?

Each prompt contains a start_time, which acts as the deadline for your response. You’ll always have at least 2 minutes from when the prompt is sent. We recommend submitting your response within 40 seconds.

#### 13. Is Synth running on a testnet too?

Yes. The testnet UID is 247, and it functions identically to mainnet.

#### 14. Once I start a miner using the guide on GitHub, where do I change the code to run my own model?

Modify this function:

[simulations.py#L10](https://github.com/mode-network/synth-subnet/blob/13642c4c3287da52c602ac8c629b26a7cdc66628/synth/miner/simulations.py#L10)

Take into account all prompt parameters except sigma, which you may ignore.


<sup>[Back to top ^][table-of-contents]</sup>

### 2.2. Useful Commands

| Command                      | Description                 |
|------------------------------|-----------------------------|
| `pm2 stop miner`             | Stops the miner.            |
| `pm2 logs miner --lines 100` | View the logs of the miner. |

<sup>[Back to top ^][table-of-contents]</sup>

### 2.3. Troubleshooting

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
