import json
import logging
import os
import sys

import pika
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(
    level="INFO",
    format="%(asctime)s  %(levelname)-8s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("uploader")


# Rabbit
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
# Raw
FANOUT_EXCHANGE_NAME = os.getenv("PRODUCER_FANOUT_EXCHANGE_NAME", "taxi_exchange")
RAW_QUEUE_NAME = os.getenv("UPLOADER_RAW_QUEUE_NAME", "uploader_raw_data_queue")
RAW_TABLE_NAME = "raw_taxi_trips"
RAW_COLUMNS = [
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
# Postgres
PROCESSED_EXCHANGE = os.getenv("PROCESSED_EXCHANGE", "processed_exchange")
PROCESSED_QUEUE = os.getenv("PROCESSED_QUEUE", "uploader_processed_data_queue")
PROCESSED_TABLE_NAME = os.getenv("PROCESSED_TABLE", "processed_taxi_trips")
PROCESSED_COLUMNS = RAW_COLUMNS + [
    "trip_duration_s",
    "avg_speed_mph",
    "pickup_hour",
    "pickup_dow",
    "pickup_month",
    "borough_pickup",
]

# Postgres
DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "taxi_data")
DB_USER = os.getenv("POSTGRES_USER", "taxi_user")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "taxi_password")

PAGE_SIZE = 5000


def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
        )
        logger.info("Connected to Postgres")
        return conn
    except psycopg2.OperationalError as e:
        logger.error("Postgres connection failed: %s", e)
        return None


def create_trips_tables(conn):
    raw_table = f"""
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
    processed_table = f"""
    CREATE TABLE IF NOT EXISTS {PROCESSED_TABLE_NAME} (
        id SERIAL PRIMARY KEY,
        vendor_id SMALLINT,
        pickup_datetime TIMESTAMP,
        dropoff_datetime TIMESTAMP,
        passenger_count SMALLINT,
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
        pickup_location_id SMALLINT,
        dropoff_location_id SMALLINT,
        split_group TEXT,
        trip_duration_s INTEGER,
        avg_speed_mph NUMERIC,
        pickup_hour SMALLINT,
        pickup_dow  SMALLINT,
        pickup_month SMALLINT,
        borough_pickup TEXT
    );"""
    try:
        with conn.cursor() as cur:
            cur.execute(raw_table)
            cur.execute(processed_table)
        conn.commit()
        logger.info(f"Data table has been created")
    except psycopg2.Error as e:
        logger.error(f"Error during creation of table: {e}")
        conn.rollback()
        raise


def bulk_insert(conn, table, cols, rows):
    if not rows:
        return 0
    sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s"
    data_iter = [tuple(r.get(c) for c in cols) for r in rows]
    with conn.cursor() as cur:
        try:
            execute_values(cur, sql, data_iter, page_size=PAGE_SIZE)
        except Exception as error:
            logger.error("Insertion error: %s", error)
            raise
    conn.commit()
    return len(rows)
    return -1


def handle_raw(ch, method, _props, body, db_conn):
    try:
        rows = json.loads(body)
        n = bulk_insert(db_conn, RAW_TABLE_NAME, RAW_COLUMNS, rows)
        logger.info("RAW inserted %d rows", n)
        ch.basic_ack(method.delivery_tag)
    except Exception:
        logger.exception("RAW insert failed – nacking")
        ch.basic_nack(method.delivery_tag, requeue=False)


def handle_processed(ch, method, _props, body, db_conn):
    try:
        rows = json.loads(body)
        n = bulk_insert(db_conn, PROCESSED_TABLE_NAME, PROCESSED_COLUMNS, rows)
        logger.info("PROCESSED inserted %d rows", n)
        ch.basic_ack(method.delivery_tag)
    except Exception:
        logger.exception("PROCESSED insert failed – nacking")
        ch.basic_nack(method.delivery_tag, requeue=False)


def main():
    db_conn = get_db_connection()
    if not db_conn:
        sys.exit(1)
    create_trips_tables(db_conn)

    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST, port=RABBITMQ_PORT, heartbeat=600
    )
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    channel.exchange_declare(FANOUT_EXCHANGE_NAME, exchange_type="fanout", durable=True)
    channel.exchange_declare(PROCESSED_EXCHANGE, exchange_type="fanout", durable=True)

    channel.queue_declare(RAW_QUEUE_NAME, durable=True)
    channel.queue_declare(PROCESSED_QUEUE, durable=True)

    channel.queue_bind(RAW_QUEUE_NAME, FANOUT_EXCHANGE_NAME)
    channel.queue_bind(PROCESSED_QUEUE, PROCESSED_EXCHANGE)

    channel.basic_qos(prefetch_count=2)

    channel.basic_consume(
        queue=RAW_QUEUE_NAME,
        on_message_callback=lambda ch, m, p, b: handle_raw(ch, m, p, b, db_conn),
    )
    channel.basic_consume(
        queue=PROCESSED_QUEUE,
        on_message_callback=lambda ch, m, p, b: handle_processed(ch, m, p, b, db_conn),
    )

    logger.info("Uploader ready...")
    try:
        channel.start_consuming()
    finally:
        if db_conn and not db_conn.closed:
            db_conn.close()
        if connection and connection.is_open:
            connection.close()


if __name__ == "__main__":
    main()
