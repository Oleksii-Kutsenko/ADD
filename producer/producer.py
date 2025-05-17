import json
import logging
import os
import random
import sys
import time

import pandas as pd
import pika

SPLIT_WEIGHTS = {"train": 0.7, "test": 0.2, "validation": 0.1}


RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
EXCHANGE_NAME = os.getenv("EXCHANGE_NAME", "taxi_exchange")
ROUTING_KEY_PROCESSOR = os.getenv("ROUTING_KEY_PROCESSOR", "raw.data.processor")
ROUTING_KEY_UPLOADER = os.getenv("ROUTING_KEY_UPLOADER", "raw.data.uploader")
INPUT_CSV_PATH = os.getenv("INPUT_CSV_PATH", "/data/taxi_trip_data.csv")
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", 1000))
DATE_COlUMNS = ["pickup_datetime", "dropoff_datetime"]

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
                exchange=exchange_name, exchange_type="direct", durable=True
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
            parse_dates=DATE_COlUMNS,
            date_format="%Y-%m-%d %H:%M:%S",
        )
        for chunk_number, chunk_df in enumerate(csv_iterator):
            chunk_prepared = chunk_df.astype(object).where(pd.notnull(chunk_df), None)

            list_of_row_dicts = chunk_prepared.to_dict(orient="records")

            rows_in_chunk_published = 0
            for row_dict in list_of_row_dicts:
                for col_name in DATE_COlUMNS:
                    assert isinstance(row_dict[col_name], pd.Timestamp), "data problem"
                    row_dict[col_name] = row_dict[col_name].isoformat()

                row_dict["split_group"] = random.choices(
                    list(SPLIT_WEIGHTS.keys()), weights=SPLIT_WEIGHTS.values(), k=1
                )[0]

                try:
                    message_json = json.dumps(row_dict)
                except TypeError as e:
                    logger.error(
                        f"Serialization error in chunk {chunk_number + 1}. Data dict: {row_dict}. Error: {e}"
                    )
                    continue

                # TODO: use fanout it should halves the time to 15 mins, and batch messages before sending them
                publish_message(
                    channel, EXCHANGE_NAME, ROUTING_KEY_PROCESSOR, message_json
                )
                publish_message(
                    channel, EXCHANGE_NAME, ROUTING_KEY_UPLOADER, message_json
                )
                rows_in_chunk_published += 1

            total_rows_published += rows_in_chunk_published
            logger.info(
                f"Processed chunk {chunk_number + 1}. Published {rows_in_chunk_published} rows. Total published: {total_rows_published}"
            )
        elapsed_time = time.time() - start_time
        logger.info(f"Finish processing. Successfully published {total_rows_published}")
        logger.info(f"Total execution time: {elapsed_time:.2f} seconds.")
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
