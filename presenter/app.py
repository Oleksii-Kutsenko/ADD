import os
from datetime import datetime

import psycopg2
from flask import Flask, request
from flask_restx import Api, Resource, fields
from psycopg2.pool import SimpleConnectionPool

DB_CFG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", 5432),
    "dbname": os.getenv("POSTGRES_DB", "taxi_data"),
    "user": os.getenv("POSTGRES_USER", "taxi_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "taxi_password"),
}
PAGE_SIZE_DEFAULT = int(os.getenv("PAGE_SIZE_DEFAULT", 100))

pool = SimpleConnectionPool(1, 5, **DB_CFG)

app = Flask(__name__)
api = Api(
    app,
    title="NYC Taxi Presenter",
    version="1.0",
    description="Serves processed_taxi_trips data",
)

trip_model = api.model(
    "Trip",
    {
        "id": fields.Integer,
        "vendor_id": fields.Integer,
        "pickup_datetime": fields.String,
        "dropoff_datetime": fields.String,
        "passenger_count": fields.Integer,
        "trip_distance": fields.Float,
        "total_amount": fields.Float,
        "split_group": fields.String,
    },
)


def fetch_all(sql, params):
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            colnames = [c.name for c in cur.description]
            return [dict(zip(colnames, row)) for row in cur.fetchall()]
    finally:
        pool.putconn(conn)


def fetch_one(sql, params):
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            if not row:
                return None
            colnames = [c.name for c in cur.description]
            return dict(zip(colnames, row))
    finally:
        pool.putconn(conn)


@api.route("/trips")
class Trips(Resource):
    @api.doc(
        params={
            "page": "Page number (1-based)",
            "page_size": "Rows per page (default 100, max 1000)",
            "start": "pickup_datetime ≥ (ISO-8601)",
            "end": "pickup_datetime < (ISO-8601)",
            "borough": "borough_pickup = value",
            "split": "split_group = train|test|validation",
        }
    )
    @api.marshal_list_with(trip_model)
    def get(self):
        # pagination
        page = max(int(request.args.get("page", 1)), 1)
        page_size = min(int(request.args.get("page_size", PAGE_SIZE_DEFAULT)), 1000)
        offset = (page - 1) * page_size

        # filters
        filters, params = [], []
        if start := request.args.get("start"):
            filters.append("pickup_datetime >= %s")
            params.append(start)
        if end := request.args.get("end"):
            filters.append("pickup_datetime < %s")
            params.append(end)
        if borough := request.args.get("borough"):
            filters.append("borough_pickup = %s")
            params.append(borough)
        if split := request.args.get("split"):
            filters.append("split_group = %s")
            params.append(split)

        where_sql = "WHERE " + " AND ".join(filters) if filters else ""
        sql = f"""
          SELECT *
          FROM processed_taxi_trips
          {where_sql}
          ORDER BY pickup_datetime
          LIMIT %s OFFSET %s
        """
        params.extend([page_size, offset])
        return fetch_all(sql, params)


@api.route("/trips/<int:trip_id>")
class TripById(Resource):
    @api.marshal_with(trip_model)
    def get(self, trip_id):
        trip = fetch_one("SELECT * FROM processed_taxi_trips WHERE id = %s", (trip_id,))
        if trip is None:
            api.abort(404, "Trip not found")
        return trip


stats_model = api.model(
    "DailyFareStat",
    {
        "pickup_date": fields.String,
        "avg_fare": fields.Float,
        "trip_count": fields.Integer,
    },
)


@api.route("/stats/daily_fare")
class DailyFare(Resource):
    @api.doc(
        params={
            "from": "YYYY-MM-DD (inclusive)",
            "to": "YYYY-MM-DD (exclusive)",
            "borough": "filter by borough_pickup",
        }
    )
    @api.marshal_list_with(stats_model)
    def get(self):
        date_from = request.args.get("from", "2018-01-01")
        date_to = request.args.get("to", "2019-01-01")
        borough = request.args.get("borough")

        sql = """
          SELECT date_trunc('day', pickup_datetime)::date AS pickup_date,
                 AVG(total_amount) AS avg_fare,
                 COUNT(*) AS trip_count
          FROM processed_taxi_trips
          WHERE pickup_datetime >= %s AND pickup_datetime < %s
          {borough_clause}
          GROUP BY pickup_date
          ORDER BY pickup_date
        """.format(
            borough_clause="AND borough_pickup = %s" if borough else ""
        )

        params = [date_from, date_to] + ([borough] if borough else [])
        return fetch_all(sql, params)


# ─────────────────────────────────────────────────────────────────── #
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
