from dataclasses import dataclass


@dataclass
class PromptConfig:
    label: str
    time_length: int
    time_increment: int
    initial_delay: int
    total_cycle_minutes: int
    timeout_extra_seconds: int
    scoring_intervals: dict[str, int]  # Define scoring intervals in seconds.


LOW_FREQUENCY = PromptConfig(
    label="low",
    time_length=86400,
    time_increment=300,
    initial_delay=60,  # avoid 2 prompts to start simultaneously
    total_cycle_minutes=60,
    timeout_extra_seconds=60,
    scoring_intervals={
        "5min": 300,  # 5 minutes
        "30min": 1800,  # 30 minutes
        "3hour": 10800,  # 3 hours
        "24hour_abs": 86400,  # 24 hours
    },
)

HIGH_FREQUENCY = PromptConfig(
    label="high",
    time_length=3600,
    time_increment=60,
    initial_delay=0,
    total_cycle_minutes=12,
    timeout_extra_seconds=60,
    scoring_intervals={
        "1min": 60,
        "2min": 120,
        "5min": 300,
        "15min": 900,
        "30min": 1800,
        "60min_abs": 3600,
        "0_5min_gaps": 300,
        "0_10min_gaps": 600,
        "0_15min_gaps": 900,
        "0_20min_gaps": 1200,
        "0_25min_gaps": 1500,
        "0_30min_gaps": 300,
        "0_35min_gaps": 300,
        "0_40min_gaps": 300,
        "0_45min_gaps": 300,
        "0_50min_gaps": 300,
        "0_55min_gaps": 300,
        "0_60min_gaps": 300,
    },
)
