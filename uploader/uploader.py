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
        return conn
    except psycopg2.OperationalError as e:
        logger.error(f"Failed to connect: {e}")
        return None

def create_raw_trips_table(conn):
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
        extra NUMERIC,
        mta_tax NUMERIC,
        tip_amount NUMERIC,
        tolls_amount NUMERIC,
        imp_surcharge NUMERIC,
        total_amount NUMERIC,
        pickup_location_id TEXT,
        dropoff_location_id TEXT,
        split_group TEXT
    )
    """
    try:
        with conn.cursor() as cur:
            cur.execute(create_table_query)
        conn.commit()
        logger.info(f"Data table has been created")
    except psycopg2.Error as e:
        logger.error(f"Error during creation of table: {e}")
        conn.rollback()
        raise 

def insert_raw_trips_batch(conn, batch_of_rows_dicts):
    if not batch_of_rows_dicts:
        return 0

    data_to_insert = []
    for row_dict in batch_of_rows_dicts:
        data_to_insert.append(tuple(row_dict.get(col) for col in RAW_TABLE_COLUMNS))

        cols_str = ", ".join(RAW_TABLE_COLUMNS)
        placeholders_str = ", ".join(["%s"] * len(RAW_TABLE_COLUMNS))
        insert_query = f"INSERT INTO {RAW_TABLE_NAME} ({cols_str}) VALUES ({placeholders_str})"

        try:
            with conn.cursor() as cur:
                cur.executemany(insert_query, data_to_insert)
            conn.commit()
            logger.info(f"Inserted {len(data_to_insert)} rows")
            return len(data_to_insert)
        except psycopg2.Error as e:
            logger.error(f"Database error: {e}")
            conn.rollback()
            return -1

def on_message_callback(ch, method, properties, body, db_connection):
    delivery_tag = method.delivery_tag
    logger.info(f"Message delivery_tag: {delivery_tag}, size: {len(body)}")

    try:
        list_or_rows = json.loads(body.decode("utf-8"))
        logger.info(f"Parsed JSON array with {len(list_or_rows)} rows.")

        # TODO: something wrong with this insert_raw_trips_batch
        inserted_count = insert_raw_trips_batch(db_connection, list_or_rows)

        if inserted_count >= 0:
            logger.info(f"Deliver tag: {delivery_tag}")
        else:
            logger.error(f"Failed to insert rows into DB. Delivery tag: {delivery_tag}")
            cn.basic_nack(delivery_tag=delivery_tag, requeue=False)
    except json.JSONDecodeError as e:
        logger.error(f"JSONDecodeError: {e}. Message body: {body[:200]}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        ch.basic_nack(delivery_tag=delivery_tag, requeue=False)

def main():
    logger.info("Listening messages")

    db_conn = get_db_connection()
    if not db_conn:
        logger.error(f"Exiting. Error", exc_info=True)
        db_conn.close()
        sys.exit(1)

    try:
        create_raw_trips_table(db_conn)
    except Exception as e:
        logger.error(f"Error during creation of table. Error: {e}")
        db_conn.close()
        sys.exit(1)

    connection_params = pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, heartbeat=600)
    try:
        connection = pika.BlockingConnection(connection_params)
        channel = connection.channel()

        channel.exchange_declare(exchange=FANOUT_EXCHANGE_NAME, exchange_type="fanout", durable=True)

        channel.queue_bind(exchange=FANOUT_EXCHANGE_NAME, queue=RAW_DATA_QUEUE_NAME)

        channel.basic_qos(prefetch_count=1)
        callback_with_db = lambda ch, method, properties, body: on_message_callback(
            ch, method, properties, body, db_conn
        )

        channel.basic_consume(queue=RAW_DATA_QUEUE_NAME, on_message_callback=callback_with_db)
        channel.start_consuming()
    except pika.exceptions.AMQPConnectionError as e:
        logger.error(f"RabbitMQ error: {e}")
    except Exception as e:
        logger.error(f"Unexpected error {e}")
    finally:
        logger.info("Shutting down")
        if 'db_conn' in locals() and db_conn and not db_conn.closed:
            db_conn.close()
            logger.info("Postgres connection closed")
        if 'connection' in locals() and connection and connection.is_open:
            connection.close()
            logger.info("RabbitMQ connection closed")

if __name__ == "__main__":
    main()

