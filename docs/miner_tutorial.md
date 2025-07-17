# Miner Tutorial

### Table of contents

* [1. Requirements](#1-requirements)
* [2. TL;DR](#2-tldr)
  - [2.1. Clone the code and implement your model](#21-clone-the-code-and-implement-your-model)
  - [2.2. Check that your model generates a valid response](#22-check-that-your-model-generates-a-valid-response)
  - [2.3. Get a VM and open port `8091`](#23-get-a-vm-and-open-port-8091)
  - [2.4. Create or import a Bittensor wallet](#24-create-or-import-a-bittensor-wallet)
  - [2.5. Launch your miner with PM2](#25-launch-your-miner-with-pm2)
  - [2.6. Verify the port is open](#26-verify-the-port-is-open)
  - [2.7. Track your miner performance](#27-track-your-miner-performance)
  - [2.8. Check your prediction validation](#28-check-your-prediction-validation)
  - [2.9. More information](#29-more-information)
* [3. Getting started](#3-getting-started)
  - [3.1. Open ports](#31-open-ports)
    - [3.1.1. Check open ports](#311-check-open-ports)
    - [3.1.2. Open using `ufw`](#312-open-using-ufw)
    - [3.1.3. Test open port](#313-test-open-port)
  - [3.2. Add ingress rules (optional)](#32-add-ingress-rules-optional)
  - [3.3. Set up the miner](#33-set-up-the-miner)
    - [3.3.1. Install dependencies](#331-install-dependencies)
    - [3.3.2. Clone the repository](#332-clone-the-repository)
    - [3.3.3. Set up & activate Python virtual environment](#333-set-up--activate-python-virtual-environment)
  - [3.4. Create a wallet](#34-create-a-wallet)
    - [3.4.1. Create the cold/hot wallets](#341-create-the-coldhot-wallets)
    - [3.4.2. Register the wallet](#342-register-the-wallet)
    - [3.4.3. Verify the wallet registration (optional)](#343-verify-the-wallet-registration-optional)
  - [3.5. Run the miner](#35-run-the-miner)
    - [3.5.1. Start the miner](#351-start-the-miner)
    - [3.5.2. Check the miner is running (optional)](#352-check-the-miner-is-running-optional)

### 1. Requirements

* [Ubuntu v20.04+](https://ubuntu.com/download)

## 2. TL;DR

### 2.1. Clone the code and implement your model

* Clone the Synth subnet repository.
* Modify the implementation of this function to run your own model: [simulations.py#L10](https://github.com/mode-network/synth-subnet/blob/13642c4c3287da52c602ac8c629b26a7cdc66628/synth/miner/simulations.py#L10)
* Use all parameters from the prompt **except** `sigma`, which you may ignore.

### 2.2. Check that your model generates a valid response

* Run this command to test your model locally:
```shell
python synth/miner/run.py
```

* If your format is correct, you’ll see the output:

```text
$ CORRECT
```

### 2.3. Get a VM and open port `8091`

* Ensure port **8091** is open in your cloud provider's **ingress rules**.
* Configure your VM's **firewall** to allow inbound traffic on this port.

### 2.4. Create or import a Bittensor wallet

* Use `btcli` to:
  - Create or import a wallet. 
  - Add funds. 
  - Register your hotkey (this will purchase a UID).

### 2.5. Launch your miner with PM2

* Create a new file called `miner.local.config.js` using the config from this link: [miner.config.js#L1](https://github.com/mode-network/synth-subnet/blob/f231c7b9151de6382d11e8102ae70c6b3f1b1fc7/miner.config.js#L1)
* Modify the wallet name and hotkey name as needed. 
* Start the miner with PM2:
```shell
pm2 start miner.local.config.js
```

### 2.6. Verify the port is open

* Your port will only be accessible if:
  - The miner is actively running.
  - Port 8091 is open on the VM and network level.
* You can verify using this tool: [https://www.yougetsignal.com/tools/open-ports](https://www.yougetsignal.com/tools/open-ports).

### 2.7. Track your miner performance

* View miner performance and stats on:
  - [Taostats (Subnet 50)](https://taostats.io/subnets/50/chart)
  - [Synth Miner Dashboard](https://miners.synthdata.co/)

### 2.8. Check your prediction validation

* View validation status of your last submission:

```text
https://api.synthdata.co/validation/miner?uid=<your UID>
```

### 2.9. More information

* Explore the full Synth API documentation: [https://api.synthdata.co](https://api.synthdata.co)

## 3. Getting started

### 3.1. Open ports

To ensure a miner can successfully connect to the network, the port `8091` **MUST** be open.

<sup>[Back to top ^][table-of-contents]</sup>

#### 3.1.1. Check open ports

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

#### 3.1.2. Open using `ufw`

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

#### 3.1.3. Test open port

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

### 3.2. Add ingress rules (optional)

If you have set up your miner on a remote server/VM using a cloud provider (GCP, AWS, Azure, e.t.c.), you will also need to add an ingress rule on port TCP/8091 to allow for incoming connections.

Please refer to your cloud provider's documentation on adding ingress rules to your server.

<sup>[Back to top ^][table-of-contents]</sup>

### 3.3. Set up the miner

#### 3.3.1. Install dependencies

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

#### 3.3.2. Clone the repository

Clone the synth subnet repository:

```shell
git clone https://github.com/mode-network/synth-subnet.git
```

Change directory to the project root

```shell
cd ./synth-subnet
```

<sup>[Back to top ^][table-of-contents]</sup>

#### 3.3.3. Set up & activate Python virtual environment

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

### 3.4. Create a wallet

> 💡 **TIP:** For a more extensive list of the Bittensor CLI commands see [here](https://docs.bittensor.com/btcli).

### 3.4.1. Create the cold/hot wallets

You will need to create the cold and hot wallets:

```shell
btcli wallet create \
  --wallet.name miner \
  --wallet.hotkey default
```

> 🚨 **WARNING:** You must ensure your wallets have enough TAO (0.25 should be enough) to be able to start mining. For testnet, check out the faucet on the Discord.

<sup>[Back to top ^][table-of-contents]</sup>

### 3.4.2. Register the wallet

Next, register the wallets by acquiring a slot on the Bittensor subnet:
```shell
btcli subnet register \
  --wallet.name miner \
  --wallet.hotkey default \
  --netuid 50
```
`
<sup>[Back to top ^][table-of-contents]</sup>

### 3.4.3. Verify the wallet registration (optional)

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

### 3.5. Run the miner

#### 3.5.1. Start the miner

Simply start PM2 with the miner config:

```shell
pm2 start miner.config.js
```

<sup>[Back to top ^][table-of-contents]</sup>

#### 3.5.2. Check the miner is running (optional)

You can check if the miner is running by using:

```shell
pm2 list
```

<sup>[Back to top ^][table-of-contents]</sup>

<!-- links -->
[table-of-contents]: #table-of-contents
