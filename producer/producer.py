import json
import logging
import os
import sys
import time

import numpy as np
import orjson
import pandas as pd
import pika

SPLITS = np.array(["train", "test", "validation"])
WEIGHTS = np.array([0.7, 0.2, 0.1])


RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
EXCHANGE_NAME = os.getenv("EXCHANGE_NAME", "taxi_exchange")
RAW_DATA_EXCHANGE_TYPE = "fanout"
INPUT_CSV_PATH = os.getenv("INPUT_CSV_PATH", "/data/taxi_trip_data.csv")

ROUTING_KEY_PROCESSOR = os.getenv("ROUTING_KEY_PROCESSOR", "raw.data.processor")
ROUTING_KEY_UPLOADER = os.getenv("ROUTING_KEY_UPLOADER", "raw.data.uploader")

CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", 59_000))
MESSAGES_BATCH_SIZE = int(os.getenv("MESSAGES_BATCH_SIZE", 59_000))


DATE_COLUMNS = ["pickup_datetime", "dropoff_datetime"]

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)


def connect_rabbitmq(host, port, exchange_name, retry_attempts=3, delay_seconds=5):
    parameters = pika.ConnectionParameters(
        host=host, port=port, heartbeat=600, blocked_connection_timeout=300
    )
    for attempt in range(retry_attempts):
        try:
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            channel.exchange_declare(
                exchange=exchange_name,
                durable=True,
                exchange_type=RAW_DATA_EXCHANGE_TYPE,
            )
            logger.info(
                f"Successfully connected to RabbitMQ on {host}:{port} and declared exchange '{exchange_name}'"
            )
            return connection, channel
        except pika.exceptions.AMQPConnectionError as connection_error:
            # Needed, don't remove
            logger.warning(
                f"RabbitMQ connection failed (Attempt {attempt +1}/{retry_attempts}): {connection_error}. Retrying in {delay_seconds}s..."
            )
            time.sleep(delay_seconds)
        logger.error("Could not connect to RabbitMQ after multiple attempts.")
        return None, None


def publish_message(channel, exchange, routing_key, message_body_json):
    try:
        channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=message_body_json,
            properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent),
        )
    except Exception as error:
        logger.error(f"Failed to push message. {error}")


def process_csv_and_publish() -> None:
    connection, channel = connect_rabbitmq(RABBITMQ_HOST, RABBITMQ_PORT, EXCHANGE_NAME)
    if not channel:
        logger.error("Exiting: Could not establish RabbitMQ channel.")
        return

    total_rows_published = 0
    start_time = time.time()

    try:
        logger.info(
            f"Starting processing CSV file: {INPUT_CSV_PATH}. Chunk size: {CHUNK_SIZE}"
        )
        csv_iterator = pd.read_csv(
            INPUT_CSV_PATH,
            chunksize=CHUNK_SIZE,
            iterator=True,
            parse_dates=DATE_COLUMNS,
            date_format="%Y-%m-%d %H:%M:%S",
        )
        for chunk_number, chunk_df in enumerate(csv_iterator):
            chunk_df["split_group"] = np.random.choice(SPLITS, len(chunk_df), p=WEIGHTS)
            chunk_df[DATE_COLUMNS] = chunk_df[DATE_COLUMNS].apply(
                lambda s: s.dt.strftime("%Y-%m-%dT%H:%M:%S")
            )
            chunk_prepared = chunk_df.astype(object).where(pd.notnull(chunk_df), None)

            rows_in_chunk_published = 0
            current_batch_of_rows = []
            for start in range(0, len(chunk_prepared), MESSAGES_BATCH_SIZE):
                batch = chunk_prepared.iloc[start : start + MESSAGES_BATCH_SIZE]
                channel.basic_publish(
                    exchange=EXCHANGE_NAME,
                    routing_key="",
                    body=orjson.dumps(batch.to_dict("records")),
                    properties=pika.BasicProperties(delivery_mode=2),
                )

            total_rows_published += len(chunk_prepared)
            logger.info(
                f"Processed chunk {chunk_number + 1}. Published {rows_in_chunk_published} rows. Total published: {total_rows_published}"
            )

            if current_batch_of_rows:
                try:
                    batch_message_json = json.dumps(current_batch_of_rows)
                    publish_message(channel, EXCHANGE_NAME, "", batch_message_json)
                    total_rows_published += len(current_batch_of_rows)
                except TypeError as e:
                    logger.error(
                        f"Serialization error in chunk {chunk_number + 1}. Error: {e}"
                    )
                    continue

        elapsed_time = time.time() - start_time
        logger.info(f"Finish processing. Successfully published {total_rows_published}")
        logger.info(f"Total execution time: {elapsed_time:.2f} seconds.")
        logger.info(f"Processing speed {total_rows_published/elapsed_time:.2f} per second")
    except FileNotFoundError:
        logger.error(f"CSV file not found {INPUT_CSV_PATH}")
    except pika.exceptions.AMQPConnectionError as connection_error:
        logger.error(f"Connection error: {connection_error}", exc_info=True)
    except Exception as unexpected_error:
        logger.error(f"Unexpected error: {unexpected_error}", exc_info=True)
    finally:
        if connection and connection.is_open:
            connection.close()
            logger.info("Connection has been successfully closed.")
    logger.info("Success")


if __name__ == "__main__":
    process_csv_and_publish()
