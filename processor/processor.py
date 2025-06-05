import json
import logging
import os
import sys

import numpy as np
import orjson
import pandas as pd
import pika

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

ESSENTIALS = ["pickup_datetime", "dropoff_datetime", "trip_distance", "total_amount"]


def load_borough_lookup(path: str) -> dict:
    df = pd.read_csv(path, usecols=["zone_id", "borough"])
    df["zone_id"] = pd.to_numeric(df["zone_id"], errors="coerce").astype("Int64")
    return df.set_index("zone_id")["borough"].to_dict()


borough_map = load_borough_lookup(GEO_LOOKUP)


def clean_and_enrich_batch(df: pd.DataFrame, borough_map: dict) -> pd.DataFrame:
    datetime_cols = ["pickup_datetime", "dropoff_datetime"]
    numeric_cols = [
        "trip_distance",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "imp_surcharge",
        "total_amount",
    ]
    df[datetime_cols] = df[datetime_cols].apply(
        pd.to_datetime, errors="coerce", utc=False
    )
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")

    df = df.dropna(subset=ESSENTIALS)
    df = df.loc[
        (df.trip_distance > 0)
        & (df.total_amount > 0)
        & (df.dropoff_datetime >= df.pickup_datetime)
    ].copy()

    df["trip_duration_s"] = (
        df.dropoff_datetime - df.pickup_datetime
    ).dt.total_seconds()
    df = df[df.trip_duration_s > 0]

    df["avg_speed_mph"] = df.trip_distance / (df.trip_duration_s / 3600.0)
    df.loc[df.avg_speed_mph > 90, "avg_speed_mph"] = np.nan

    df["pickup_hour"] = df.pickup_datetime.dt.hour.astype("int8")
    df["pickup_dow"] = df.pickup_datetime.dt.dayofweek.astype("int8")
    df["pickup_month"] = df.pickup_datetime.dt.month.astype("int8")

    df["borough_pickup"] = df.pickup_location_id.map(borough_map).astype("string")

    # ISO-8601 datetime strings
    for col in datetime_cols:
        df[col] = df[col].dt.strftime("%Y-%m-%dT%H:%M:%S")

    df = df.where(pd.notnull(df), None)
    return df.reset_index(drop=True)


def publish_batch(channel, records_list):
    for start in range(0, len(records_list), PAGE_SIZE):
        payload = orjson.dumps(records_list[start : start + PAGE_SIZE])
        channel.basic_publish(
            exchange=PROC_EXCHANGE,
            routing_key=ROUTING_KEY,
            body=payload,
            properties=pika.BasicProperties(delivery_mode=2),
        )


def on_message(ch, method, _props, body: bytes):
    tag = method.delivery_tag
    try:
        df = pd.DataFrame(orjson.loads(body))
        cleaned_df = clean_and_enrich_batch(df, borough_map)
        records = cleaned_df.to_dict("records")
        publish_batch(ch, records)
        ch.basic_ack(tag)
        logger.info("Processed %d → published %d rows", len(df), len(cleaned_df))
    except Exception:
        logger.exception("processing failed – nacking")
        ch.basic_nack(tag, requeue=False)


def main():
    logger.info("Processor starting…")
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST, port=RABBITMQ_PORT, heartbeat=600
    )
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    channel.exchange_declare(RAW_EXCHANGE, "fanout", durable=True)
    channel.exchange_declare(PROC_EXCHANGE, "fanout", durable=True)
    channel.queue_declare(RAW_QUEUE, durable=True)
    channel.queue_bind(RAW_QUEUE, exchange=RAW_EXCHANGE)

    channel.basic_qos(prefetch_count=4)
    channel.basic_consume(queue=RAW_QUEUE, on_message_callback=on_message)

    logger.info("Waiting for raw batches…")
    try:
        channel.start_consuming()
    finally:
        connection.close()


if __name__ == "__main__":
    main()
