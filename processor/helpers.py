import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

ESSENTIALS = ["pickup_datetime", "dropoff_datetime", "trip_distance", "total_amount"]


def load_borough_lookup(path: str) -> dict:
    try:
        df = pd.read_csv(path, usecols=["zone_id", "borought"])
        df["zone_id"] = pd.to_numeric(df["zone_id"], errors="coerce").astype("Int64")
        return df.set_index("zone_id")["borought"].to_dict()
    except Exception:
        logger.exception("Failed to load lookup file")
        return None


def clean_and_enrich_batch(df: pd.Dataframe, borough_map: dict) -> None:
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

    return df.reset_index(drop=True)
