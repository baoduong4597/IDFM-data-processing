#!/usr/bin/env python3
import os
from typing import List, Dict, Any
from datetime import datetime, date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# If not installed yet:
#   pip install influxdb-client
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


# ====================================================
# 1) Configuration
# ====================================================

# Path to curated parquet (output of Kafka -> raw -> curated job)
CURATED_PATH = os.environ.get(
    "CURATED_PATH",
    "/data/idfm/curated/stopmonitoring_passages"
)

# InfluxDB v2 connection settings (must be set in environment)
INFLUX_URL = os.environ.get("INFLUX_URL", "http://localhost:8086")
INFLUX_ORG = os.environ.get("INFLUX_ORG", "esilv-bigdata")
INFLUX_BUCKET = os.environ.get("INFLUX_BUCKET", "IDFM")
INFLUX_TOKEN = os.environ.get("INFLUX_TOKEN")  # must be provided

# Measurement name in InfluxDB
MEASUREMENT_NAME = os.environ.get("INFLUX_MEASUREMENT", "idfm_passages_v3")


# Columns treated as tags (low-cardinality dimensions)
TAG_COLUMNS: List[str] = [
    "roi_label",
    "short_name",        # line short name (A, E, 7, 9, 14, 310, 320)
    "transport_mode",    # rail / metro / bus
    "stop_point_name",   # human-readable stop name
    "destination_name",   # terminus (ref) - we want to filter by direction
    # you could also add "direction_ref" if useful
]

# Columns used as time candidates for the InfluxDB point
TIME_PRIORITY: List[str] = [
    "expected_departure_time",
    "recorded_at_time",
    "producer_ts",
    "ingest_ts",
]


# ====================================================
# 2) Spark session
# ====================================================

def create_spark() -> SparkSession:
    """
    Create a SparkSession. The master (yarn / local / standalone)
    is configured via spark-submit, not in code.
    """
    spark = (
        SparkSession.builder
        .appName("IDFM_Curated_To_InfluxDB")
        .getOrCreate()
    )
    return spark


# ====================================================
# 3) Load curated dataframe and pre-filter
# ====================================================

def load_curated_df(spark: SparkSession):
    """
    Load curated parquet and apply minimal filtering:
    - expected_departure_time is not null (we need it for time-based KPIs)
    """
    print(f"[INFO] Reading curated parquet from: {CURATED_PATH}")
    df = spark.read.parquet(CURATED_PATH)

    print("[INFO] Curated schema:")
    df.printSchema()

    # Filter only rows with an expected departure time
    df_kpi = df.filter(F.col("expected_departure_time").isNotNull())

    total = df_kpi.count()
    print(f"[INFO] KPI dataframe count: {total}")

    return df_kpi


# ====================================================
# 4) Convert Spark rows to InfluxDB points
# ====================================================

def choose_point_time(row_dict: Dict[str, Any]) -> datetime:
    """
    Choose the timestamp for the InfluxDB point using a priority order:
    expected_departure_time -> recorded_at_time -> producer_ts -> ingest_ts -> now()
    """
    for col in TIME_PRIORITY:
        value = row_dict.get(col)
        if value is not None:
            # Spark timestamps come as Python datetime already
            return value
    # Fallback: current UTC time
    return datetime.utcnow()


def build_points_from_rows(rows) -> List[Point]:
    """
    Convert a list of Row objects into a list of InfluxDB Point objects.
    All curated fields are kept as fields except:
      - TAG_COLUMNS -> tags
      - time columns (TIME_PRIORITY) -> only used as measurement time
    We also add an 'influx_inserted_at' field with the ingestion time.
    """
    points: List[Point] = []
    now_str = datetime.utcnow().isoformat()

    for row in rows:
        row_dict = row.asDict()

        # Build tags
        tags: Dict[str, str] = {}
        for col in TAG_COLUMNS:
            val = row_dict.get(col)
            if val is not None:
                tags[col] = str(val)

        # Choose time
        point_time = choose_point_time(row_dict)

        # Build fields: all remaining columns except tags and time columns
        fields: Dict[str, Any] = {}
        for col, val in row_dict.items():
            if col in TAG_COLUMNS:
                continue
            if col in TIME_PRIORITY:
                continue
            if val is None:
                continue

            # Normalize field types for Influx
            if isinstance(val, bool):
                fields[col] = val
            elif isinstance(val, (int, float)):
                fields[col] = val
            elif isinstance(val, date) and not isinstance(val, datetime):
                # Spark 'date' type -> ISO string
                fields[col] = val.isoformat()
            else:
                # Fallback: string
                fields[col] = str(val)

        # Add an explicit ingestion field
        fields["influx_inserted_at"] = now_str

        # Build the InfluxDB Point
        p = Point(MEASUREMENT_NAME)
        for k, v in tags.items():
            p = p.tag(k, v)
        for k, v in fields.items():
            p = p.field(k, v)

        p = p.time(point_time, WritePrecision.NS)
        points.append(p)

    return points


# ====================================================
# 5) Write points to InfluxDB
# ====================================================

def write_points_to_influx(points: List[Point]):
    """
    Write the list of points to InfluxDB using the configured bucket/org.
    """
    if not INFLUX_TOKEN:
        raise RuntimeError("INFLUX_TOKEN is not set in environment")

    print(f"[INFO] Number of points to write to InfluxDB: {len(points)}")
    if not points:
        print("[WARN] No points to write. Exiting without calling InfluxDB.")
        return

    print(f"[INFO] Connecting to InfluxDB at {INFLUX_URL}")
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)

    try:
        write_api = client.write_api(write_options=SYNCHRONOUS)
        print(f"[INFO] Writing points to bucket='{INFLUX_BUCKET}', org='{INFLUX_ORG}'")
        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points)
        print("[INFO] Write to InfluxDB completed successfully.")
    finally:
        client.close()
        print("[INFO] InfluxDB client closed.")


# ====================================================
# 6) Main
# ====================================================

def main():
    print("=========== CURATED -> INFLUXDB CONFIG ===========")
    print("CURATED_PATH     :", CURATED_PATH)
    print("INFLUX_URL       :", INFLUX_URL)
    print("INFLUX_ORG       :", INFLUX_ORG)
    print("INFLUX_BUCKET    :", INFLUX_BUCKET)
    print("MEASUREMENT_NAME :", MEASUREMENT_NAME)
    print("Tag columns      :", TAG_COLUMNS)
    print("Time priority    :", TIME_PRIORITY)
    print("==================================================")

    if not INFLUX_TOKEN:
        raise RuntimeError("INFLUX_TOKEN is not set in environment")

    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:
        df_kpi = load_curated_df(spark)

        # Collect rows into driver memory (OK for small demo dataset)
        print("[INFO] Collecting KPI dataframe into driver memory…")
        rows = df_kpi.collect()
        print(f"[INFO] Number of rows to convert to points: {len(rows)}")

        print("[INFO] Converting rows to InfluxDB points…")
        points = build_points_from_rows(rows)
        print(f"[INFO] Number of points built: {len(points)}")

        write_points_to_influx(points)

    finally:
        print("[INFO] Stopping Spark session…")
        spark.stop()
        print("[INFO] Spark session stopped.")


if __name__ == "__main__":
    main()
