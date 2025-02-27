import logging
import math
from typing import List, Dict, Tuple

import yaml

# Rich imports for pretty printing.
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import box

# Create a console instance for Rich.
console = Console()

# Configure logging.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("staking.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# Global variable to record the total TAO allocated by the script.
TOTAL_ALLOCATED = 0.0

# Global dictionaries to store the last observed volume and the moving average of volume delta.
last_volume_dict: Dict[int, float] = {}
avg_vol_delta_dict: Dict[int, float] = {}
# Smoothing factor for the moving average of volume delta.
VOLUME_ALPHA = 0.1


def read_config() -> Dict:
    """
    Read configuration from 'config.yaml' in the local folder.
    Expected keys:
      wallet: string
      amount_staked: float (TAO to stake)
      amount_unstaked: float (TAO value to unstake)
      validator: string (the validator key)
      ranks_file: string (path to the YAML file with subnet rankings)
      ranking_beta: float
      drive: float (drive factor multiplier)
    """
    try:
        with open("dca/config.yaml", "r") as f:
            config = yaml.safe_load(f)
        return config
    except Exception as e:
        logger.critical(f"Failed to read dac/config.yaml: {e}")
        raise


def read_ranks_file(filename: str) -> List[int]:
    """
    Read a YAML file where the key 'ranks' contains an ordered list of subnet UIDs.
    """
    try:
        with open(filename, "r") as f:
            data = yaml.safe_load(f)
        if "ranks" not in data:
            raise ValueError("Key 'ranks' not found in the ranks file.")
        return data["ranks"]
    except Exception as e:
        logger.critical(f"Failed to read ranks file {filename}: {e}")
        raise


def compute_weights_from_ranks(
    ranks: List[int], beta: float
) -> Dict[int, float]:
    """
    Given an ordered list of subnet UIDs and a beta value, compute a weight for each.
    For a subnet at index idx, score = (N - idx) where N is total count.
    Then:
      weight = exp(beta * score) / sum(exp(beta * score))
    Returns a dict mapping netuid to computed weight.
    """
    N = len(ranks)
    scores = [N - idx for idx in range(N)]
    exp_scores = [math.exp(beta * s) for s in scores]
    total_exp = sum(exp_scores)
    normalized_weights = [s / total_exp for s in exp_scores]
    weight_dict = {
        netuid: normalized_weights[i] for i, netuid in enumerate(ranks)
    }
    return weight_dict


async def get_subnet_stats(
    sub,
    allowed_subnets: List[int],
    weight_dict: Dict[int, float],
    drive: float,
) -> Tuple[Dict[int, Dict], Dict[int, int]]:
    """
    Fetch all subnet data and compute stats for each allowed subnet.
    For each subnet, compute:
      - price, emission
      - raw yield = (emission - price) / emission
      - boost = weight * drive
      - score = (emission*(1 + boost) - price) / (emission*(1 + boost))
    Also record subnet name and volume.
    Returns:
      - stats: dict mapping netuid to its stats.
      - rank_dict: dict mapping netuid to its rank (by price).
    """
    all_subnets = await sub.all_subnets()

    # Rank subnets by price descending.
    sorted_subnets = sorted(
        all_subnets, key=lambda s: float(s.price), reverse=True
    )
    rank_dict = {s.netuid: idx + 1 for idx, s in enumerate(sorted_subnets)}

    stats = {}
    for subnet in all_subnets:
        netuid = subnet.netuid
        if netuid == 0 or netuid not in allowed_subnets:
            continue

        price = float(subnet.price)
        if price <= 0:
            continue
        emission = float(subnet.tao_in_emission)
        raw_yield = (emission - price) / emission
        weight = weight_dict.get(netuid, 1)
        boost = weight * drive
        # Compute score using effective emission boost:
        score = (emission * (1 + boost) - price) / (emission * (1 + boost))
        name = (
            str(subnet.subnet_name) if hasattr(subnet, "subnet_name") else ""
        )
        volume = (
            float(subnet.subnet_volume)
            if hasattr(subnet, "subnet_volume")
            else 0.0
        )
        stats[netuid] = {
            "price": price,
            "emission": emission,
            "raw_yield": raw_yield,
            "weight": weight,
            "boost": 1 + boost,
            "score": score,
            "name": name,
            "volume": volume,
        }
    return stats, rank_dict


def print_table_rich(
    stake_info: Dict,
    allowed_subnets: List[int],
    stats: Dict[int, Dict],
    rank_dict: Dict[int, int],
    balance: float,
    total,
):
    """
    Print a Rich table with columns:
      Subnet | Name | Boost | Yield | Score | Vol Delta | Emission | Price | Stake | Stake Value | Rank
    All floating-point values are formatted to 4-decimal precision.
    The Vol Delta column displays the moving average of the volume change per block.
    """
    global last_volume_dict, avg_vol_delta_dict

    total_stake_value = 0.0
    total_stake = 0.0

    table = Table(
        title="Staking Allocations",
        header_style="bold white on dark_blue",
        box=box.SIMPLE_HEAVY,
    )
    table.add_column("Subnet", justify="right", style="bright_cyan")
    table.add_column("Name", justify="left", style="white")
    table.add_column("Boost", justify="right", style="yellow")
    table.add_column("Yield", justify="right", style="cyan")
    table.add_column("Score", justify="right", style="bright_magenta")
    table.add_column("Vol Delta", justify="right", style="bright_red")
    table.add_column("Emission", justify="right", style="red")
    table.add_column("Price", justify="right", style="green")
    table.add_column("Stake", justify="right", style="magenta")
    table.add_column("Stake Value", justify="right", style="bright_green")
    table.add_column("Rank", justify="right", style="bright_blue")

    for netuid in allowed_subnets:
        stake_obj = stake_info.get(netuid)
        stake_amt = float(stake_obj.stake) if stake_obj is not None else 0.0
        total_stake += stake_amt

        if netuid in stats:
            price = float(stats[netuid]["price"])
            raw_yield = float(stats[netuid]["raw_yield"])
            boost = float(stats[netuid]["boost"])
            score = float(stats[netuid]["score"])
            emission = float(stats[netuid]["emission"])
            name = stats[netuid].get("name", "")
            current_volume = float(stats[netuid]["volume"])
        else:
            price = raw_yield = boost = score = emission = 0.0
            name = ""
            current_volume = 0.0

        # Update moving average of volume delta.
        last_vol = last_volume_dict.get(netuid, current_volume)
        raw_delta = current_volume - last_vol
        # Get previous average delta; if none, use the current delta.
        prev_avg = avg_vol_delta_dict.get(netuid, raw_delta)
        avg_delta = VOLUME_ALPHA * raw_delta + (1 - VOLUME_ALPHA) * prev_avg
        avg_vol_delta_dict[netuid] = avg_delta
        last_volume_dict[netuid] = current_volume

        rank = int(rank_dict.get(netuid, 0))
        stake_value = stake_amt * price
        total_stake_value += stake_value

        table.add_row(
            str(netuid),
            name,
            f"{boost:.4f}",
            f"{raw_yield:.4f}",
            f"{score:.4f}",
            f"{avg_delta:.4f}",
            f"{emission:.4f}",
            f"{price:.4f}",
            f"{stake_amt:.4f}",
            f"{stake_value:.4f}",
            str(rank),
        )

    table.add_row(
        "[bold]TOTAL[/bold]",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        f"[bold]{total_stake:.4f}[/bold]",
        f"[bold]{total_stake_value:.4f}[/bold]",
        "",
    )

    summary = (
        f"[bold cyan]Wallet Balance:[/bold cyan] {balance:.4f} TAO    "
        f"[bold cyan]Total TAO (un)Allocated:[/bold cyan] {total:.4f} TAO    "
        f"[bold cyan]Total Stake Value:[/bold cyan] {total_stake_value:.4f} TAO"
    )
    console.print(Panel(summary, style="bold white"))
    console.print(table)
