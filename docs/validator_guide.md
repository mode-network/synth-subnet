# Validator

### Table Of Contents

* [1. Create a Wallet](#1-create-a-wallet)
* [2. Run the Validator](#2-run-the-validator)
* [3. Appendix](#3-appendix)
  - [3.1. Useful Commands](#31-useful-commands)

## 1. Create a Wallet

1. (Optional) If you haven't already, ensure you are running from the Python virtual environment:
```shell
source bt_venv/bin/activate
```

2. Create a cold key wallet:
```shell
btcli wallet <replace_with_wallet_name> \
  --wallet.name validator
```

3. Create a hot key wallet:
```shell
btcli wallet <replace_with_wallet_name> \
  --wallet.name validator \
  --wallet.hotkey default
```

4. Acquire a slot on the Bittensor subnet by registering the wallet:
```shell
btcli subnet register \
  --wallet.name validator \
  --wallet.hotkey default \
  --subtensor.network <replace_with_network_name> \
  --netuid <replace_with_netud>
```

5. (Optional) Check the wallet has been registered:
```shell
btcli wallet overview \
  --wallet.name validator \
  --subtensor.network <replace_with_network_name>
```

6. (Optional) Check the network metagraph:
```shell
btcli subnet metagraph \
  --subtensor.network <replace_with_network_name> \
  --netuid <replace_with_netud>
```

<sup>[Back to top ^][table-of-contents]</sup>

## 2. Run the Validator

1. (Optional) If you haven't already, ensure you are running from the Python virtual environment:
```shell
source bt_venv/bin/activate
```

2. Start PM2 with the validator config:
```shell
pm2 start validator.config.js
```

3. (Optional) Check the validator is running:
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
