import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, DateTime, JSON, Float, String, BigInteger
from sqlalchemy.dialects.postgresql import JSONB

# Load environment variables from .env file
load_dotenv()

# Database connection
DATABASE_URL = os.getenv('DB_URL')
engine = create_engine(DATABASE_URL)
metadata = MetaData()

# Define the table
validator_requests = Table(
    'validator_requests',
    metadata,
    Column("id", BigInteger, primary_key=True),
    Column("start_time", DateTime(timezone=True), nullable=False),
    Column("asset", String, nullable=True),
    Column("time_increment", Integer, nullable=True),
    Column("time_length", Integer, nullable=True),
    Column("num_simulations", Integer, nullable=True),
)

# Define the table
miner_predictions = Table(
    "miner_predictions",
    metadata,
    Column("id", BigInteger, primary_key=True),
    Column("validator_requests_id", BigInteger, nullable=False),
    Column("miner_uid", Integer, nullable=False),
    Column("prediction", JSONB, nullable=False),
)

# Define the table
miner_scores = Table(
    "miner_scores",
    metadata,
    Column("id", BigInteger, primary_key=True),
    Column("miner_uid", Integer, nullable=False),
    Column("scored_time", DateTime(timezone=True), nullable=False),
    Column("miner_predictions_id", BigInteger, nullable=False),
    Column("reward", Float, nullable=False),
    Column("reward_details", JSONB, nullable=False),
    Column("real_prices", JSON, nullable=False),
)
