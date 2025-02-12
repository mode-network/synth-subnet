import os
import logging

from dotenv import load_dotenv
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Integer,
    DateTime,
    JSON,
    Float,
    String,
    BigInteger,
)
from sqlalchemy.dialects.postgresql import JSONB


def get_database_url():
    """Returns the database URL from environment variables."""
    load_dotenv()
    db_url = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
    return db_url


def create_database_engine():
    """Creates and returns a new database engine."""
    database_url = get_database_url()
    if not database_url:
        raise ValueError("invalid postgres environment variables.")
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.pool").setLevel(logging.WARNING)
    engine = create_engine(database_url, echo=False)
    return engine


metadata = MetaData()
db_engine = None


def get_engine():
    """Lazy-load and return the global database engine."""
    global db_engine
    if db_engine is None:
        db_engine = create_database_engine()
    return db_engine


# Define the table
validator_requests = Table(
    "validator_requests",
    metadata,
    Column("id", BigInteger, primary_key=True),
    Column("start_time", DateTime(timezone=True), nullable=False),
    Column("asset", String, nullable=True),
    Column("time_increment", Integer, nullable=True),
    Column("time_length", Integer, nullable=True),
    Column("num_simulations", Integer, nullable=True),
    Column("request_time", DateTime(timezone=True), nullable=True),
)

# Define the table
miner_predictions = Table(
    "miner_predictions",
    metadata,
    Column("id", BigInteger, primary_key=True),
    Column("validator_requests_id", BigInteger, nullable=False),
    Column("miner_uid", Integer, nullable=False),
    Column("prediction", JSONB, nullable=False),
    Column("format_validation", String, nullable=True),
    Column("process_time", Float, nullable=True),
)

# Define the table
miner_scores = Table(
    "miner_scores",
    metadata,
    Column("id", BigInteger, primary_key=True),
    Column("miner_uid", Integer, nullable=False),
    Column("scored_time", DateTime(timezone=True), nullable=False),
    Column("miner_predictions_id", BigInteger, nullable=False),
    Column("prompt_score", Float, nullable=False),
    Column("score_details", JSONB, nullable=False),
    Column("real_prices", JSON, nullable=False),
)

# Define the table
miner_rewards = Table(
    "miner_rewards",
    metadata,
    Column("id", BigInteger, primary_key=True),
    Column("miner_uid", Integer, nullable=False),
    Column("smoothed_score", Float, nullable=False),
    Column("reward_weight", Float, nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)

# Define the table
metagraph_history = Table(
    "metagraph_history",
    metadata,
    Column("id", BigInteger, primary_key=True),
    Column("neuron_uid", Integer, nullable=False),
    Column("incentive", Float, nullable=True),
    Column("rank", Float, nullable=True),
    Column("stake", Float, nullable=True),
    Column("trust", Float, nullable=True),
    Column("emission", Float, nullable=True),
    Column("coldkey", String, nullable=True),
    Column("hotkey", String, nullable=True),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)

# Define the table
weights_update_history = Table(
    "weights_update_history",
    metadata,
    Column("id", BigInteger, primary_key=True),
    Column("miner_uids", JSON, nullable=False),
    Column("miner_weights", JSON, nullable=False),
    Column("norm_miner_uids", JSON, nullable=False),
    Column("norm_miner_weights", JSON, nullable=False),
    Column("update_result", String, nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)
