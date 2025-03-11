from datetime import datetime, timedelta

from synth.miner.simulations import generate_simulations
from synth.simulation_input import SimulationInput
from synth.validator import response_validation
from synth.validator.miner_data_handler import MinerDataHandler


def generate_values(start_time: datetime):
    """Generate values for the given start_time."""
    values = []
    for i in range(0, 24 * 60, 5):  # 5-minute intervals for 1 day
        time_point = (start_time + timedelta(minutes=i)).isoformat()
        price = 90000 + i * 100  # Random price
        values.append({"time": time_point, "price": price})

    return [values]


def prepare_random_predictions(db_engine, start_time):
    handler = MinerDataHandler(db_engine)
    miner_uids = [0, 1, 2, 3]

    simulation_input = SimulationInput(
        asset="BTC",
        start_time=start_time,
        time_increment=300,
        time_length=86400,
        num_simulations=1,
    )

    simulation_data = {
        miner_uids[0]: (
            generate_simulations(start_time=start_time),
            response_validation.CORRECT,
            "1.2",
        ),
        miner_uids[1]: (
            generate_simulations(start_time=start_time),
            response_validation.CORRECT,
            "3",
        ),
        miner_uids[2]: (
            generate_simulations(start_time=start_time),
            response_validation.CORRECT,
            "15",
        ),
        miner_uids[3]: (
            generate_simulations(start_time=start_time),
            "time out or internal server error (process time is None)",
            "2.1",
        ),
    }
    handler.save_responses(simulation_data, simulation_input, datetime.now())

    return handler, simulation_input, miner_uids
