# Miner Tutorial

### Table of contents

* [1. Requirements](#1-requirements)
* [2. Getting started](#2-getting-started)
  - [2.1. Open ports](#21-open-ports)
    - [2.1.1. Check open ports](#211-check-open-ports)
    - [2.1.2. Open using `ufw`](#212-open-using-ufw)
    - [2.1.3. Test open port](#213-test-open-port)
  - [2.2. Add ingress rules (optional)](#22-add-ingress-rules-optional)
  - [2.3. Set up the miner](#23-set-up-the-miner)
    - [2.3.1. Install dependencies](#231-install-dependencies)
    - [2.3.2. Clone the repository](#232-clone-the-repository)
    - [2.3.3. Set up & activate Python virtual environment](#233-set-up--activate-python-virtual-environment)
  - [2.4. Create a wallet](#24-create-a-wallet)
    - [2.4.1. Create the cold/hot wallets](#241-create-the-coldhot-wallets)
    - [2.4.2. Register the wallet](#242-register-the-wallet)
    - [2.4.3. Verify the wallet registration (optional)](#243-verify-the-wallet-registration-optional)
  - [2.5. Run the miner](#25-run-the-miner)
    - [2.5.1. Start the miner](#251-start-the-miner)
    - [2.5.2. Check the miner is running (optional)](#252-check-the-miner-is-running-optional)

### 1. Requirements

* [Ubuntu v20.04+](https://ubuntu.com/download)

## 2. Getting started

### 2.1. Open ports

To ensure a miner can successfully connect to the network, the port `8091` **MUST** be open.

<sup>[Back to top ^][table-of-contents]</sup>

#### 2.1.1. Check open ports

Before the beginning, check what ports are open:

```shell
nmap localhost
```

which should output:

```text
$ nmap localhost

Starting Nmap 7.80 ( https://nmap.org ) at 2025-07-15 12:43 CEST
Nmap scan report for localhost (127.0.0.1)
Host is up (0.000079s latency).
Not shown: 998 closed ports
PORT    STATE SERVICE
22/tcp  open  ssh
80/tcp  open  http
631/tcp open  ipp

Nmap done: 1 IP address (1 host up) scanned in 0.04 seconds
```

> ⚠️ **NOTE**: You can install `nmap` via `sudo apt install nmap`.

<sup>[Back to top ^][table-of-contents]</sup>

#### 2.1.2. Open using `ufw`

It is **RECOMMENDED** that `ufw` (Uncomplicated Firewall) is used to handle port connections. 

`ufw` is a minimal front-end for managing iptables rules. It allows you to easily open ports with simple commands

First, enable `ufw` using:

```shell
sudo ufw enable
```

Next, allow incoming traffic on the correct port:

```shell
sudo ufw allow 8091
```

To ensure the port is accessible and the rule is active, execute:

```shell
sudo ufw status
```

which should output:

```text
$ sudo ufw status

Status: active

To                         Action      From
--                         ------      ----
8091                       ALLOW       Anywhere                  
8091 (v6)                  ALLOW       Anywhere (v6) 
```

<sup>[Back to top ^][table-of-contents]</sup>

#### 2.1.3. Test open port

Using `nmap` you can check if the port is open using:

```shell
nmap -p 8091 localhost
```

which should output:

```text
$ nmap -p 8091 localhost

Starting Nmap 7.80 ( https://nmap.org ) at 2025-07-15 12:50 CEST
Nmap scan report for localhost (127.0.0.1)
Host is up (0.000073s latency).

PORT     STATE  SERVICE
8091/tcp open jamlink

Nmap done: 1 IP address (1 host up) scanned in 0.03 seconds
```

<sup>[Back to top ^][table-of-contents]</sup>

### 2.2. Add ingress rules (optional)

If you have set up your miner on a remote server/VM using a cloud provider (GCP, AWS, Azure, e.t.c.), you will also need to add an ingress rule on port TCP/8091 to allow for incoming connections.

Please refer to your cloud provider's documentation on adding ingress rules to your server.

<sup>[Back to top ^][table-of-contents]</sup>

### 2.3. Set up the miner

#### 2.3.1. Install dependencies

Install rust:

```shell
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Add the required `apt` repositories:

```shell
sudo add-apt-repository ppa:deadsnakes/ppa
```

> ⚠️ **NOTE:** The [deadsnakes](https://github.com/deadsnakes) repository, while unofficial, it is hugely popular and used by many Python projects.


Install Python and Node/npm:

```shell
sudo apt update && \
  sudo apt install nodejs npm python3.10 python3.10-venv pkg-config
```

Install [PM2](https://pm2.io/) via npm:

```shell
sudo npm install pm2 -g
```

<sup>[Back to top ^][table-of-contents]</sup>

#### 2.3.2. Clone the repository

Clone the synth subnet repository:

```shell
git clone https://github.com/mode-network/synth-subnet.git
```

Change directory to the project root

```shell
cd ./synth-subnet
```

<sup>[Back to top ^][table-of-contents]</sup>

#### 2.3.3. Set up & activate Python virtual environment

Create a new Python virtual environment:

```shell
python3.10 -m venv bt_venv
```

Activate and switch to the newly created Python virtual environment:

```shell
source bt_venv/bin/activate
```

> ⚠️ **NOTE**: This should activate the `bt_venv` environment, and you will see the command line prefixed with `(bt_venv)`.

Install local Python dependencies within the virtual environment:

```shell
pip install -r requirements.txt
```

<sup>[Back to top ^][table-of-contents]</sup>

### 2.4. Create a wallet

> 💡 **TIP:** For a more extensive list of the Bittensor CLI commands see [here](https://docs.bittensor.com/btcli).

### 2.4.1. Create the cold/hot wallets

You will need to create the cold and hot wallets:

```shell
btcli wallet create \
  --wallet.name miner \
  --wallet.hotkey default
```

> 🚨 **WARNING:** You must ensure your wallets have enough TAO (0.1 should be enough) to be able to start mining. For testnet, you can use the [`btcli wallet faucet`](https://docs.bittensor.com/btcli#btcli-wallet-faucet).

<sup>[Back to top ^][table-of-contents]</sup>

### 2.4.2. Register the wallet

Next, register the wallets by acquiring a slot on the Bittensor subnet:
```shell
btcli subnet register \
  --wallet.name miner \
  --wallet.hotkey default \
  --netuid 50
```
`
<sup>[Back to top ^][table-of-contents]</sup>

### 2.4.3. Verify the wallet registration (optional)

You can verify the wallet registration by running:
```shell
btcli wallet overview \
  --wallet.name miner \
  --wallet.hotkey default
```

And, you can also check the network metagraph:
```shell
btcli subnet metagraph \
  --netuid 50
```

<sup>[Back to top ^][table-of-contents]</sup>

### 2.5. Run the miner

#### 2.5.1. Start the miner

Simply start PM2 with the miner config:

```shell
pm2 start miner.config.js
```

<sup>[Back to top ^][table-of-contents]</sup>

#### 2.5.2. Check the miner is running (optional)

You can check if the miner is running by using:

```shell
pm2 list
```

<sup>[Back to top ^][table-of-contents]</sup>

<!-- links -->
[table-of-contents]: #table-of-contents
