import os

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

# Load environment variables from .env file
load_dotenv()

# Database connection
DATABASE_URL = os.getenv("DB_URL")
engine = create_engine(DATABASE_URL)
metadata = MetaData()

# Define the table
miner_predictions = Table(
    "miner_predictions",
    metadata,
    Column("id", BigInteger, primary_key=True),
    Column("miner_uid", Integer, nullable=False),
    Column("start_time", DateTime(timezone=True), nullable=False),
    Column("asset", String, nullable=True),
    Column("time_increment", Integer, nullable=True),
    Column("time_length", Integer, nullable=True),
    Column("num_simulations", Integer, nullable=True),
    Column("prediction", JSONB, nullable=False),
)

# Define the table
miner_rewards = Table(
    "miner_rewards",
    metadata,
    Column("id", BigInteger, primary_key=True),
    Column("miner_uid", Integer, nullable=False),
    Column("start_time", DateTime(timezone=True), nullable=False),
    Column("reward_details", JSONB, nullable=False),
    Column("reward", Float, nullable=False),
    Column("real_prices", JSON, nullable=False),
    Column("prediction", JSON, nullable=False),
)
