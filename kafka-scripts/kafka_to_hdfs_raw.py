#!/usr/bin/env python3
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    to_date
)

# ============================================
# 1) Configuration
# ============================================

# Kafka bootstrap servers (same as producer)
KAFKA_BOOTSTRAP = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS",
    "master:9092,worker1:9092",
)

# Kafka topic where the raw StopMonitoring events are produced
TOPIC = os.environ.get("STOPMON_TOPIC", "idfm_stop_monitoring_raw")

# HDFS base path for raw layer (Parquet)
RAW_OUTPUT_PATH = os.environ.get(
    "RAW_OUTPUT_PATH",
    "/data/idfm/raw/stopmonitoring"
)

# HDFS path for Spark Structured Streaming checkpoints
CHECKPOINT_PATH = os.environ.get(
    "RAW_CHECKPOINT_PATH",
    "/data/idfm/checkpoints/stopmonitoring_raw"
)


# ============================================
# 2) Build SparkSession
# ============================================

def create_spark() -> SparkSession:
    """
    Create SparkSession for Kafka → HDFS raw ingestion.
    The master (yarn / standalone cluster) is configured
    via spark-submit, not in code.
    """
    spark = (
        SparkSession.builder
        .appName("IDFM_StopMonitoring_Raw_Ingestion")
        .getOrCreate()
    )
    return spark


# ============================================
# 3) Main streaming logic
# ============================================

def main():
    print("=========== RAW INGEST CONFIG ===========")
    print("Kafka bootstrap   :", KAFKA_BOOTSTRAP)
    print("Kafka topic       :", TOPIC)
    print("Raw output path   :", RAW_OUTPUT_PATH)
    print("Checkpoint path   :", CHECKPOINT_PATH)
    print("========================================")

    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    # ----------------------------------------
    # Read from Kafka as a streaming source
    # ----------------------------------------
    df_kafka = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        # Use "earliest" for testing to read from the beginning,
        # switch to "latest" later if needed.
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Kafka schema includes:
    # key (binary), value (binary), topic, partition, offset, timestamp, etc.

    # ----------------------------------------
    # Select and cast the columns we want to store in the raw layer
    # ----------------------------------------
    df_raw = (
        df_kafka
        .select(
            col("key").cast("string").alias("kafka_key"),
            col("value").cast("string").alias("raw_json"),
            col("topic").alias("kafka_topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),
            current_timestamp().alias("ingest_ts"),
        )
        .withColumn("ingest_date", to_date(col("ingest_ts")))
    )

    # ----------------------------------------
    # Write stream to HDFS as Parquet
    # ----------------------------------------
    query = (
        df_raw
        .writeStream
        .format("parquet")
        .option("path", RAW_OUTPUT_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        # Append mode is the standard for Kafka → HDFS ingestion.
        .outputMode("append")
        # Small micro-batch interval, you can adjust if needed.
        .trigger(processingTime="30 seconds")
        .partitionBy("ingest_date")
        .start()
    )

    print("[INFO] Streaming query started. Waiting for termination…")
    query.awaitTermination()


if __name__ == "__main__":
    main()
