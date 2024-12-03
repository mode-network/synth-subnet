
# Synth Subnet

## Intro

The Synth Subnet leverages Bittensor’s decentralized intelligence network to create the world's most powerful synthetic data for price forecasting. Unlike traditional price prediction systems that focus on single-point forecasts, Synth specializes in capturing the full distribution of possible price movements and their associated probabilities, to build the most accurate synthetic data in the world. 

Miners in the network are tasked with generating multiple simulated price paths, which must accurately reflect real-world price dynamics including volatility clustering and fat-tailed distributions. Their predictions are evaluated using the Continuous Ranked Probability Score (CRPS), which measures both the calibration and sharpness of their forecasts against actual price movements.  

Validators score miners on short-term and long-term prediction accuracy, with recent performance weighted more heavily through an exponential decay function. Daily emissions are allocated based on miners’ relative performance, creating a competitive environment that rewards consistent accuracy.  

The Synth Subnet aims to become a key source of synthetic price data for AI Agents and the go-to resource for options trading and portfolio management, offering valuable insights into price probability distributions.


## 1. Task Presented to the Miners

Miners are tasked with providing probabilistic forecasts of a cryptocurrency's future price movements. Specifically, each miner is required to generate multiple simulated price paths for an asset, from the current time over specified time increments and time horizon. Initially all checking prompts will be to produce 100 simulated paths for the future price of bitcoin at 5-minute time increments for the next 24 hours. 

Whereas other subnets ask miners to predict single values for future prices, we’re interested in the miners correctly quantifying uncertainty. We want their price paths to represent their view of the probability distribution of the future price, and we want their paths to encapsulate realistic price dynamics, such as volatility clustering and skewed fat tailed price change distributions. Subsequently we’ll expand to requesting forecasts for multiple assets, where modelling the correlations between the asset prices will be essential.

If the miners do a good job, the Synth Subnet will become the world-leading source of realistic synthetic price data for training AI agents. And it will be the go-to location for asking questions on future price probability distributions - a valuable resource for options trading and portfolio management.

The checking prompts sent to the miners will have the format:
(start_time, asset, time_increment, time_horizon, num_simulations)

Initially prompt parameters will always have the following values:
- **Start Time ($t_0$)**: 1 minute from the time of the request.
- **Asset**: BTC
- **Time Increment ($\Delta t$)**: 5 minutes.
- **Time Horizon ($T$)**: 24 hours.
- **Number of Simulations ($N_{\text{sim}}$)**: 100.


The miner has until the start time to return ($N_{\text{sim}}$) paths, each containing price predictions at times given by:

$$
t_i = t_0 + i \times \Delta t, \quad \text{for }\, i = 0, 1, 2, \dots, N
$$

where:

- $N = \dfrac{T}{\Delta t}$ is the total number of increments.


We recommend the miner sends a request to the Pyth Oracle to acquire the price of the asset at the start_time.

If they fail to return predictions by the start_time or the predictions are in the wrong format, they will be scored 0 for that prompt.


## 2. Validator's Scoring Methodology

The role of the validators is, after the time horizon as passed, to judge the accuracy of each miner’s predicted paths compared to how the price moved in reality. The validator evaluates the miners' probabilistic forecasts using the Continuous Ranked Probability Score (CRPS). The CRPS is a proper scoring rule that measures the accuracy of probabilistic forecasts for continuous variables, considering both the calibration and sharpness of the predicted distribution. The lower the CRPS, the better the forecasted distribution predicted the observed value.


### Application of CRPS to Ensemble Forecasts

In our setup, miners produce ensemble forecasts by generating a finite number of simulated price paths rather than providing an explicit continuous distribution. The CRPS can be calculated directly from these ensemble forecasts using an empirical formula suitable for finite samples.

For a single observation $x$ and an ensemble forecast consisting of $N$ members $y_1, y_2, \dots, y_N$, the CRPS is calculated as:

$$
\text{CRPS} = \frac{1}{N}\sum_{n=1}^N \left| y_n - x \right| - \frac{1}{2N^2} \sum_{n=1}^N \sum_{m=1}^N \left| y_n - y_m \right|
$$

where:

- The first term $\dfrac{1}{N}\sum_{n=1}^N \left| y_n - x \right|$ measures the average absolute difference between the ensemble members and the observation $x$.
- The second term $\dfrac{1}{2N^2} \sum_{n=1}^N \sum_{m=1}^N \left| y_n - y_m \right|$ accounts for the spread within the ensemble, ensuring the score reflects the ensemble's uncertainty.


This formulation allows us to assess the miners' forecasts directly from their simulated paths without the need to construct an explicit probability distribution.


### Application to Multiple Time Increments

To comprehensively assess the miners' forecasts, the CRPS is applied to sets of price changes over different time increments. These increments include short-term and long-term intervals (in the case of the initial checking prompt parameters, these will be 5 minutes, 30 minutes, 3 hours, 24 hours).

For each time increment:
- **Predicted Price Changes**: The miners' ensemble forecasts are used to compute predicted price changes over the specified intervals
- **Observed Price Changes**: The real asset prices are used to calculate the observed price changes over the same intervals. We recommend the validators collect and store the prices by sending requests to the Pyth oracle at each time increment, to be used at the end of the time horizon.
- **CRPS Calculation**: The CRPS is calculated for each increment by comparing the ensemble of predicted price changes to the observed price change.
  
The final score for a miner for a single checking prompt is the sum of these CRPS values over all the time increments.


## 3. Calculation of Leaderboard Score

### Normalization Using Softmax Function

After calculating the sum of the CRPS values, the validator normalizes these scores across all miners who submitted correctly formatted forecasts prior to the start time. The normalized score $S_i$ for miner $i$ is calculated as:

$$
S_i = \frac{e^{-\beta \cdot CRPS_i}}{\sum_j e^{-\beta \cdot CRPS_j}}
$$

where:
- $CRPS_i$ is the sum of CRPS values for miner $i$ on that day
- $\beta = \frac{1}{1000}$ is the scaling factor
- The negative sign ensures better forecasts (lower CRPS) receive higher scores

Any miners who didn’t submit a correct prediction are allocated a normalised score of 0 for that prompt.


### Exponentially Decaying Time-Weighted Average (Leaderboard Score)

The validator is required to store the historic request scores for each miner. After each new request is scored, the validator recalculates the ‘leaderboard score’ for each miner, using an exponentially decaying time-weighted average over their past **per request** scores, up to a threshold of 30 days in the past.

This approach emphasizes recent performance while still accounting for historical scores. 
The leaderboard score for miner $i$ at time $t$ is calculated as:

$$
L_i(t) = \frac{\sum_{j} w_j \, S_{i,j}}{\sum_{j} w_j}
$$

where:

- $S_{i,j}$ is the normalized score of miner $i$ at request $j$.
- $w_j = e^{-\lambda (t - t_j)}$ is the weight assigned to the score $S_{i,j}$.
- $t_j$ is the time of request $j$.
- $\lambda = \dfrac{\ln 2}{h}$ is the decay constant, with half-life $h = 10$ days.
- The sum runs over all requests $j$ such that $t - t_j \leq T$, where $T = 30$ days is the threshold time.


### Allocation of Emissions 
At the end of each day, the leaderboard scores are then raised to the power of an exponent $\alpha$ (e.g., $\alpha = 2$) to amplify performance differences. The adjusted scores determine each miner's share of the total emissions for that day

Adjusted Scores:

$$
AdjScore_{i,t} = (L_{i,t})^\alpha
$$

Emissions Allocation:

$$
P_{i,t} = \frac{AdjScore_{i,t}}{\sum_j AdjScore_{j,t}} \times Total
$$



## Overall Purpose

The system creates a competitive environment through:

1. **Implementing CRPS Scoring**
   - Objectively measures forecast quality across multiple time increments

2. **Using Ensemble Forecasts**
   - Calculates CRPS from finite ensemble of simulations

3. **Applying CRPS to Different Time Increments**
   - Evaluates both short-term and long-term predictions

4. **Normalizing Scores**
   - Ensures fair comparison using softmax function ($\beta = \frac{1}{1000}$)

5. **Calculating Leaderboard Scores and Allocating Emissions**
   - Rewards consistent performance and encourages competition


## Environment Setup Instructions

### Step 1: Create a Wallet
You can create a single wallet or multiple wallets, depending on how many miners or validators you intend to run.  
There is no functional difference between wallets for miners or validators, and you can name them as you prefer.

#### Create a Miner Wallet
- create a coldkey
```
btcli wallet new_coldkey --wallet.name miner
```

- create a hotkey
```
btcli wallet new_hotkey --wallet.name miner --wallet.hotkey default
```

#### Create a Validator Wallet
- create a coldkey
```
btcli wallet new_coldkey --wallet.name validator
```

- create a hotkey
```
btcli wallet new_hotkey --wallet.name validator --wallet.hotkey default
```

---

### Step 2: Register Wallet in the Subnet
Before running a miner or a validator you have to acquire a slot in the subnet,  
this way you connect your miner or validator application to the bittensor subnet:
```
# miner registration example
btcli subnet register --wallet.name miner --wallet.hotkey default --subtensor.network test

# validator registration example
btcli subnet register --wallet.name validator --wallet.hotkey default --subtensor.network test
```

### Check the registration
```
# miner wallet details
btcli wallet overview --wallet.name miner --subtensor.network test

# validator wallet details
btcli wallet overview --wallet.name validator --subtensor.network test
```

### Check the metagraph of the network
```
btcli subnet metagraph --netuid 247 --subtensor.network test
```

### Install PM2
It is recommended to run miner and validator using "pm2":
```
sudo apt update
sudo apt install nodejs npm
sudo npm install pm2 -g
```
Verify installation:
```
pm2 --version
```

### Synth repository
You'll have to clone the repository where you can find an example miner and validator.  
You can update the code or implement your own, using the base template defined in the repository:
```
git clone https://github.com/mode-network/synth-subnet.git
```

### Python virtual environment and dependencies
```
# install python 3.9, do not uninstall or upgrade if you have the default 3.8 or any other in the system
sudo apt install python3.9

# install venv package
sudo apt install python3.9-venv

# activate virtual environment
python3.9 -m venv bt_venv
source bt_venv/bin/activate

# install dependencies
pip install -r requirements.txt

# add the current directory to PYTHONPATH
# we had an issue that modules in the root couldn't be found without it
export PYTHONPATH="/home/{your-user}/synth-subnet:$PYTHONPATH"
```

### Running a Miner
You can find *.config.js files in the root of repository. These are configuration files used to define and manage miner and validator applications.  
They contain structured information about how an application should be started, run, and managed by PM2. They allow you to specify application details, environment variables, runtime configurations, and more.

IMPORTANT: Make sure your have activated your virtual environment before running your miner.

To run your miner:
```
pm2 start miner.config.js
```

You can find another file (this miner returns constant data and used for testing):
```
pm2 start miner-dummy.config.js
```

You can create your own configuration file for a miner and run it in the same way.

Convenient commands for pm2:
```
# returns a list of currently running applications
pm2 list

# stop the application (you can stop an applicaion by name you can see in the previous command output)
pm2 stop miner

# start the application (you can also start the application by name if you run it before)
# it is convenient if you had some changes in the code of this application
pm2 start miner

# look at the logs
pm2 logs miner

# if the last command output logs is not enough you can specify the amount like this
pm2 logs miner --lines 100
```

### Running a Validator
You can find *.config.js files in the root of repository. These are configuration files used to define and manage miner and validator applications.  
They contain structured information about how an application should be started, run, and managed by PM2. They allow you to specify application details, environment variables, runtime configurations, and more.

IMPORTANT: Make sure your have activated your virtual environment before running your miner.

To run your validator:
```
pm2 start validator.config.js
```
