#!/usr/bin/env python3
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    explode_outer,
    to_timestamp,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
    ArrayType,
)


# ============================================
# 1) Configuration
# ============================================

# HDFS raw path (where the Kafka → Parquet job writes)
RAW_INPUT_PATH = os.environ.get(
    "RAW_OUTPUT_PATH",
    "/data/idfm/raw/stopmonitoring"
)

# Curated output path
CURATED_OUTPUT_PATH = os.environ.get(
    "CURATED_OUTPUT_PATH",
    "/data/idfm/curated/stopmonitoring_passages"
)

# Optional: restrict to one date (e.g. 2026-01-01) for testing
INGEST_DATE_FILTER = os.environ.get("INGEST_DATE_FILTER", "")  # format: yyyy-MM-dd

# ROI lines CSV (from previous step)
ROI_LINES_CSV = os.environ.get(
    "ROI_LINES_CSV",
    "/user/adm-mcsc/kafka-scripts/reference/roi_lines.csv"
)


# ============================================
# 2) Spark session
# ============================================

def create_spark() -> SparkSession:
    """
    Create a SparkSession for batch curated job.
    The cluster master (yarn / standalone) is configured via spark-submit.
    """
    spark = (
        SparkSession.builder
        .appName("IDFM_StopMonitoring_Curated_Batch")
        .getOrCreate()
    )
    return spark


# ============================================
# 3) JSON schema for raw_json
# ============================================

def build_event_schema() -> StructType:
    """
    Schema for the event stored in raw_json.
    Only the fields we actually need are declared.
    """
    # Array of objects with "value"
    arr_value = ArrayType(
        StructType([
            StructField("value", StringType(), True)
        ]),
        True
    )

    monitored_call_schema = StructType([
        StructField("StopPointName", arr_value, True),
        StructField("VehicleAtStop", BooleanType(), True),
        StructField("DestinationDisplay", arr_value, True),
        StructField("ExpectedDepartureTime", StringType(), True),
        StructField("DepartureStatus", StringType(), True),
        StructField("ArrivalStatus", StringType(), True),
    ])

    monitored_vehicle_journey_schema = StructType([
        StructField("LineRef",
                    StructType([StructField("value", StringType(), True)]),
                    True),
        StructField("DirectionName", arr_value, True),
        StructField("DestinationRef",
                    StructType([StructField("value", StringType(), True)]),
                    True),
        StructField("DestinationName", arr_value, True),
        StructField("MonitoredCall", monitored_call_schema, True),
        StructField("DirectionRef",
                    StructType([StructField("value", StringType(), True)]),
                    True),
        StructField("DestinationShortName", arr_value, True),
    ])

    monitored_stop_visit_schema = StructType([
        StructField("RecordedAtTime", StringType(), True),
        StructField("ItemIdentifier", StringType(), True),
        StructField("MonitoringRef",
                    StructType([StructField("value", StringType(), True)]),
                    True),
        StructField("MonitoredVehicleJourney",
                    monitored_vehicle_journey_schema,
                    True),
    ])

    stop_monitoring_delivery_schema = StructType([
        StructField("ResponseTimestamp", StringType(), True),
        StructField("Version", StringType(), True),
        StructField("Status", StringType(), True),
        StructField("MonitoredStopVisit",
                    ArrayType(monitored_stop_visit_schema, True),
                    True),
    ])

    service_delivery_schema = StructType([
        StructField("ResponseTimestamp", StringType(), True),
        StructField("ProducerRef", StringType(), True),
        StructField("ResponseMessageIdentifier", StringType(), True),
        StructField("StopMonitoringDelivery",
                    ArrayType(stop_monitoring_delivery_schema, True),
                    True),
    ])

    siri_schema = StructType([
        StructField("ServiceDelivery", service_delivery_schema, True),
    ])

    payload_schema = StructType([
        StructField("Siri", siri_schema, True),
    ])

    event_schema = StructType([
        StructField("event_source", StringType(), True),
        StructField("event_version", IntegerType(), True),
        StructField("producer_ts", StringType(), True),
        StructField("monitoring_ref", StringType(), True),
        StructField("payload", payload_schema, True),
    ])

    return event_schema


# ============================================
# 4) Main curated logic
# ============================================

def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("=========== CURATED BATCH CONFIG ===========")
    print("RAW_INPUT_PATH       :", RAW_INPUT_PATH)
    print("CURATED_OUTPUT_PATH  :", CURATED_OUTPUT_PATH)
    print("ROI_LINES_CSV        :", ROI_LINES_CSV)
    print("INGEST_DATE_FILTER   :", INGEST_DATE_FILTER or "(none)")
    print("============================================")

    # -----------------------------
    # 4.1 Load raw parquet
    # -----------------------------
    df_raw = spark.read.parquet(RAW_INPUT_PATH)

    if INGEST_DATE_FILTER:
        df_raw = df_raw.where(col("ingest_date") == INGEST_DATE_FILTER)

    print("[INFO] Raw rows:", df_raw.count())

    # -----------------------------
    # 4.2 Parse raw_json with schema
    # -----------------------------
    event_schema = build_event_schema()

    df_parsed = (
        df_raw
        .select(
            "kafka_key",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "ingest_ts",
            "ingest_date",
            from_json(col("raw_json"), event_schema).alias("event")
        )
    )

    df_ev = (
        df_parsed
        .select(
            "kafka_key",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "ingest_ts",
            "ingest_date",
            col("event.event_source").alias("event_source"),
            col("event.event_version").alias("event_version"),
            col("event.producer_ts").alias("producer_ts_raw"),
            col("event.monitoring_ref").alias("monitoring_ref"),
            col("event.payload.Siri.ServiceDelivery.StopMonitoringDelivery")
                .alias("stop_monitoring_delivery")
        )
    )

    # -----------------------------
    # 4.3 Explode StopMonitoringDelivery and MonitoredStopVisit
    # -----------------------------
    df_smd = (
        df_ev
        .withColumn("smd", explode_outer("stop_monitoring_delivery"))
        .drop("stop_monitoring_delivery")
    )

    df_visits = (
        df_smd
        .withColumn("msv", explode_outer("smd.MonitoredStopVisit"))
    )

    # -----------------------------
    # 4.4 Extract curated columns
    # -----------------------------
    df_curated = (
        df_visits
        .select(
            # ingestion / event metadata
            "ingest_date",
            "ingest_ts",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "event_source",
            "event_version",
            "monitoring_ref",
            "kafka_key",

            # producer_ts, recorded_at_time
            to_timestamp(col("producer_ts_raw"),
                         "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
                .alias("producer_ts"),
            to_timestamp(col("msv.RecordedAtTime"),
                         "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
                .alias("recorded_at_time"),

            # vehicle journey
            col("msv.ItemIdentifier").alias("item_identifier"),
            col("msv.MonitoredVehicleJourney.LineRef.value").alias("line_ref"),
            col("msv.MonitoredVehicleJourney.DirectionRef.value").alias("direction_ref"),
            col("msv.MonitoredVehicleJourney.DirectionName")[0]["value"]
                .alias("direction_name"),
            col("msv.MonitoredVehicleJourney.DestinationRef.value")
                .alias("destination_ref"),
            col("msv.MonitoredVehicleJourney.DestinationName")[0]["value"]
                .alias("destination_name"),

            # call at this stop
            col("msv.MonitoredVehicleJourney.MonitoredCall.StopPointName")[0]["value"]
                .alias("stop_point_name"),
            to_timestamp(
                col("msv.MonitoredVehicleJourney.MonitoredCall.ExpectedDepartureTime"),
                "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
            ).alias("expected_departure_time"),
            col("msv.MonitoredVehicleJourney.MonitoredCall.DepartureStatus")
                .alias("departure_status"),
            col("msv.MonitoredVehicleJourney.MonitoredCall.VehicleAtStop")
                .alias("vehicle_at_stop"),
        )
    )

    # -----------------------------
    # 4.5 Join with ROI lines metadata
    # -----------------------------
    df_lines = (
        spark.read
        .option("header", True)
        .csv(ROI_LINES_CSV)
    )

    # df_lines has: roi_label, line_id, line_code, siri_line_ref, name, short_name, transport_mode, ...
    df_lines_small = df_lines.select(
        col("siri_line_ref").alias("join_line_ref"),
        "roi_label",
        "name",
        "short_name",
        "transport_mode",
    )

    df_curated_with_line = (
        df_curated
        .join(
            df_lines_small,
            df_curated["line_ref"] == df_lines_small["join_line_ref"],
            "left"
        )
        .drop("join_line_ref")
    )

    # Optional: keep only visits on ROI lines (non-null roi_label)
    df_final = df_curated_with_line.filter(col("roi_label").isNotNull())

    print("[INFO] Curated rows (ROI lines only):", df_final.count())

    # -----------------------------
    # 4.6 Write curated parquet
    # -----------------------------
    (
        df_final
        .write
        .mode("overwrite")
        .partitionBy("ingest_date", "roi_label")
        .parquet(CURATED_OUTPUT_PATH)
    )

    print("[INFO] Curated data written to:", CURATED_OUTPUT_PATH)


if __name__ == "__main__":
    main()
