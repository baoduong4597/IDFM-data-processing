#!/usr/bin/env python3
import os
import json
from datetime import datetime, timezone

from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


# =========================
# Config (match your setup)
# =========================
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "master:9092,worker1:9092").split(",")
TOPIC = os.environ.get("HADOOP_METRICS_TOPIC", "hadoop-metrics")
CONSUMER_GROUP = os.environ.get("HADOOP_METRICS_GROUP", "grafana-metrics-consumer")

INFLUX_URL = os.environ.get("INFLUX_URL", "http://localhost:8086")
INFLUX_ORG = os.environ.get("INFLUX_ORG", "esilv-bigdata")
INFLUX_BUCKET = os.environ.get("INFLUX_BUCKET", "IDFM")  # You can set to "hadoop_metrics" if you create a new bucket
INFLUX_TOKEN = os.environ.get("INFLUX_TOKEN")  # required

MEASUREMENT = os.environ.get("INFLUX_MEASUREMENT", "hadoop")


def main():
    if not INFLUX_TOKEN:
        raise RuntimeError("Missing env var INFLUX_TOKEN")

    print("=========== Kafka -> Influx Metrics Consumer ===========")
    print("Kafka bootstrap :", KAFKA_BOOTSTRAP)
    print("Topic           :", TOPIC)
    print("Consumer group  :", CONSUMER_GROUP)
    print("Influx URL      :", INFLUX_URL)
    print("Influx Org      :", INFLUX_ORG)
    print("Influx Bucket   :", INFLUX_BUCKET)
    print("Measurement     :", MEASUREMENT)
    print("=======================================================")

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="latest",     # don't replay old data unless you want to
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    try:
        for msg in consumer:
            payload = msg.value  # producer_metrics.py sends dict like {"clusterMetrics": {...}}
            cm = payload.get("clusterMetrics", {})

            # Pick a timestamp for the point (use now as "sampling time")
            ts = datetime.now(timezone.utc)

            p = Point(MEASUREMENT).time(ts, WritePrecision.NS)

            # Tags (optional but useful)
            p = p.tag("source", "yarn_rm") \
                 .tag("rm_host", "master") \
                 .tag("topic", TOPIC)

            # Fields (numbers only for Grafana panels)
            # Memory
            if "totalMB" in cm: p = p.field("totalMB", int(cm["totalMB"]))
            if "availableMB" in cm: p = p.field("availableMB", int(cm["availableMB"]))
            if "allocatedMB" in cm: p = p.field("allocatedMB", int(cm["allocatedMB"]))
            if "reservedMB" in cm: p = p.field("reservedMB", int(cm["reservedMB"]))
            if "pendingMB" in cm: p = p.field("pendingMB", int(cm["pendingMB"]))
            if "utilizedMBPercent" in cm: p = p.field("utilizedMBPercent", float(cm["utilizedMBPercent"]))

            # vCores
            if "totalVirtualCores" in cm: p = p.field("totalVirtualCores", int(cm["totalVirtualCores"]))
            if "availableVirtualCores" in cm: p = p.field("availableVirtualCores", int(cm["availableVirtualCores"]))
            if "allocatedVirtualCores" in cm: p = p.field("allocatedVirtualCores", int(cm["allocatedVirtualCores"]))
            if "utilizedVirtualCoresPercent" in cm: p = p.field("utilizedVirtualCoresPercent", float(cm["utilizedVirtualCoresPercent"]))

            # Apps / Nodes / Containers
            for k in [
                "appsSubmitted","appsCompleted","appsPending","appsRunning","appsFailed","appsKilled",
                "totalNodes","activeNodes","lostNodes","unhealthyNodes",
                "containersAllocated","containersReserved","containersPending",
                "rmSchedulerBusyPercent",
            ]:
                if k in cm:
                    # some are ints, some are percents -> cast safely
                    v = cm[k]
                    if isinstance(v, bool):
                        p = p.field(k, bool(v))
                    elif isinstance(v, (int, float)):
                        p = p.field(k, float(v) if "Percent" in k else int(v))
                    else:
                        # ignore non-numeric
                        pass

            write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=p)
            print(f"[OK] wrote point @ {ts.isoformat()} (partition={msg.partition}, offset={msg.offset})")

    finally:
        client.close()


if __name__ == "__main__":
    main()
