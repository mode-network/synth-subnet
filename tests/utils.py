from datetime import datetime, timedelta


def generate_values(start_time: datetime):
    """Generate values for the given miner_id and start_time."""
    values = []
    for i in range(0, 24 * 60, 5):  # 5-minute intervals for 1 day
        time_point = (start_time + timedelta(minutes=i)).isoformat()
        price = 90000 + i * 100  # Random price
        values.append({"time": time_point, "price": price})

    return values
