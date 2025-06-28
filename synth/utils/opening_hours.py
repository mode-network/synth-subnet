import pytz
from datetime import datetime, timedelta


def should_skip_xau(start_time: datetime) -> bool:
    """
    Determines whether to skip the XAU (Gold) market based on the current time.

    Returns:
        bool: True if the XAU market should be skipped, False otherwise.
    """
    ny_time = start_time.astimezone(pytz.timezone("America/New_York"))

    # Find the most recent Friday at 17:00
    days_since_friday = (ny_time.weekday() - 4) % 7  # Friday is 4
    last_friday = (ny_time - timedelta(days=days_since_friday)).replace(
        hour=17, minute=0, second=0, microsecond=0
    )

    # Saturday at 17:00 following most recent Friday 17:00
    saturday_17 = last_friday + timedelta(days=1)

    # Check if current time is between Friday 17:00 and Saturday 17:00
    return last_friday <= ny_time < saturday_17
