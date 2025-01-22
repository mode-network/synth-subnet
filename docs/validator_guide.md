# Validator

### Table Of Contents

* [1. Create a Wallet](#1-create-a-wallet)
* [2. Run the Validator](#2-run-the-validator)
* [3. Appendix](#3-appendix)
  - [3.1. Useful Commands](#31-useful-commands)

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
  --subtensor.network test \
  --netuid 247
```

```shell
btcli root register --wallet.name validator --wallet.hotkey default --network test
```

4. Stake:
```shell
btcli stake add \
  --wallet.name validator \
  --wallet.hotkey default \
  --subtensor.network test \
  --netuid 247
```

**Step 4: Verify wallet registration (optional)**

Check the wallet has been registered:
```shell
btcli wallet overview \
  --wallet.name validator \
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

## 2. Run the Validator

**Step 1: Activate the Python virtual environment**

```shell
source bt_venv/bin/activate
```

**Step 2: Start PM2 with the validator config**

```shell
pm2 start validator.config.js
```

**Step 2: Check the validator is running (optional)**

```shell
pm2 list
```

<sup>[Back to top ^][table-of-contents]</sup>

## 3. Appendix

### 3.1. Useful Commands

| Command                          | Description                     |
|----------------------------------|---------------------------------|
| `pm2 stop validator`             | Stops the validator.            |
| `pm2 logs validator --lines 100` | View the logs of the validator. |

<sup>[Back to top ^][table-of-contents]</sup>

<!-- links -->
[table-of-contents]: #table-of-contents
