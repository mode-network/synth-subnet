import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, DateTime, JSON
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import sessionmaker
from datetime import datetime

# Load environment variables from .env file
load_dotenv()

# Database connection
DATABASE_URL = os.getenv('DB_URL')
engine = create_engine(DATABASE_URL)
metadata = MetaData()

# Define the table
miner_predictions = Table(
    "miner_predictions",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("miner_uid", Integer, nullable=False),
    Column("start_time", DateTime(timezone=True), nullable=False),
    Column("prediction", JSONB, nullable=False),
)
