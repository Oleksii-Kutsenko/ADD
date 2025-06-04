import json
import logging
import os
import sys

import pandas as pd
import pika
from helpers import clean_and_enrich_batch, load_borough_lookup

logging.basicConfig(
    level="INFO",
    format="%(asctime)s  %(levelname)-8s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("processor")


RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", 5672))
RAW_QUEUE = os.getenv("RAW_QUEUE", "processor_raw_data_queue")
RAW_EXCHANGE = os.getenv("RAW_EXCHANGE", "taxi_exchange")
PROC_EXCHANGE = os.getenv("PROC_EXCHANGE", "processed_exchange")
ROUTING_KEY = os.getenv("ROUTING_KEY_UPLOADER", "processed.data.uploader")
PAGE_SIZE = int(os.getenv("PAGE_SIZE", 10_000))
GEO_LOOKUP = os.getenv("GEO_LOOKUP_PATH", "/archive/taxi_zone_geo.csv")

borough_map = load_borough_lookup(GEO_LOOKUP)


def publish_batch(channel, records):
    for start in range(0, len(records), PAGE_SIZE):
        slice_ = records[start : start + PAGE_SIZE]
        channel.basic_publish(
            exchange=PROC_EXCHANGE,
            routing_key=ROUTING_KEY,
            body=json.dumps(slice_).encode(),
            properties=pika.BasicProperties(delivery_mode=2),
        )


def on_message(ch, method, _props, body: bytes):
    delivery_tag = method.delivery_tag
    try:
        raw_rows = json.loads(body)
        df = pd.DataFrame(raw_rows)
        cleaned = clean_and_enrich_batch(df, borough_map)
        publish_batch(ch, cleaned.to_dict("records"))
        ch.basic_ack(delivery_tag)
        logger.info("Processed %s → published %s rows", len(df), len(cleaned))
    except Exception:
        logger.exception("processing failed – nacking")
        ch.basic_nack(delivery_tag, requeue=False)


def main():
    logger.info("Processor starting…")
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST, port=RABBITMQ_PORT, heartbeat=600
    )
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    channel.exchange_declare(
        exchange=RAW_EXCHANGE, exchange_type="fanout", durable=True
    )
    channel.exchange_declare(
        exchange=PROC_EXCHANGE, exchange_type="fanout", durable=True
    )
    channel.queue_declare(queue=RAW_QUEUE, durable=True)
    channel.queue_bind(queue=RAW_QUEUE, exchange=RAW_EXCHANGE)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=RAW_QUEUE, on_message_callback=on_message)

    logger.info("Waiting for raw batches…")
    try:
        channel.start_consuming()
    finally:
        connection.close()


if __name__ == "__main__":
    main()
