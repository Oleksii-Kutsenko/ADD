import json
import logging
import os
import sys
import time

import pika
import psycopg2
from psycopg2 import extras

logging.basicConfig(
    level="INFO",
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# Rabbit
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
FANOUT_EXCHANGE_NAME = os.getenv("PRODUCER_FANOUT_EXCHANGE_NAME", "taxi_exchange")
RAW_DATA_QUEUE_NAME = os.getenv("UPLOADER_RAW_QUEUE_NAME", "uploader_raw_data_queue")
# Postgres
DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "taxi_data")
DB_USER = os.getenv("POSTGRES_USER", "taxi_user")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "strong_password")
RAW_TABLE_NAME = "raw_taxi_trips"
RAW_TABLE_COLUMNS = [
    "vendor_id",
    "pickup_datetime",
    "dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "rate_code",
    "store_and_fwd_flag",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "imp_surcharge",
    "total_amount",
    "pickup_location_id",
    "dropoff_location_id",
    "split_group",
]


def get_db_connection():
    conn_params = {
        "host": DB_HOST,
        "port": DB_PORT,
        "dbname": DB_NAME,
        "user": DB_USER,
        "password": DB_PASSWORD,
    }
    try:
        conn = psycopg2.connect(**conn_params)
        logger.info(f"Connected to {DB_NAME}")
    except psycopg2.OperationalError as e:
        logger.error(f"Failed to connect: {e}")
        return None

def create_raw_trips_table(conn):
    # TODO: Finish query
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {RAW_TABLE_NAME} (
        id SERIAL PRIMARY KEY,
        vendor_id INTEGER,
        pickup_datetime TIMESTAMP WITHOUT TIME ZONE,
        dropoff_datetime TIMESTAMP WITHOUT TIME ZONE,
        passenger_count INTEGER,
        trip_distance NUMERIC,
        rate_code TEXT,
        store_and_fwd_flag TEXT,
        payment_type TEXT,
        fare_amount NUMERIC,
        
    )
    """
