#!/usr/bin/env python3
import os
import json
import csv
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer

# ============================================
# 1) Configuration
# ============================================

IDFM_API_KEY = os.environ.get("IDFM_API_KEY")
if not IDFM_API_KEY:
    raise RuntimeError("IDFM_API_KEY is not set in environment")

# Kafka cluster bootstrap servers
KAFKA_BOOTSTRAP = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS",
    "master:9092,worker1:9092"
).split(",")

# Kafka topic for raw StopMonitoring events
TOPIC = os.environ.get("STOPMON_TOPIC", "idfm_stop_monitoring_raw")

# CSV file with ROI stops (exported previously)
ROI_STOPS_CSV = os.environ.get(
    "ROI_STOPS_CSV",
    "./kafka-scripts/reference/roi_stops.csv"
)

# StopMonitoring endpoint
STOPMONITORING_URL = "https://prim.iledefrance-mobilites.fr/marketplace/stop-monitoring"


# ============================================
# 2) Kafka Producer
# ============================================

def create_producer() -> KafkaProducer:
    """
    Create and return a KafkaProducer instance with JSON value serialization.
    """
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda v: v.encode("utf-8") if v is not None else None,
    )
    return producer


# ============================================
# 3) Read MonitoringRef list from roi_stops.csv
# ============================================

def iter_roi_monitoring_refs(csv_path: str):
    """
    Read the roi_stops.csv file and yield:
        (monitoring_ref, row_dict)

    The CSV is expected to contain a column: "siri_monitoring_ref".
    """
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            monitoring_ref = (row.get("siri_monitoring_ref") or "").strip()
            if not monitoring_ref:
                continue
            yield monitoring_ref, row


# ============================================
# 4) Call StopMonitoring API
# ============================================

def call_stop_monitoring(monitoring_ref: str) -> dict:
    """
    Call the IDFM StopMonitoring endpoint for a given MonitoringRef
    and return the parsed JSON response.
    """
    params = {
        "MonitoringRef": monitoring_ref,
    }
    headers = {
        "accept": "application/json",
        "apiKey": IDFM_API_KEY,
    }

    print(f"[INFO] Calling StopMonitoring for {monitoring_ref}")
    resp = requests.get(
        STOPMONITORING_URL,
        headers=headers,
        params=params,
        timeout=20
    )
    print("[INFO] HTTP status:", resp.status_code)
    print("[INFO] Final URL   :", resp.url)

    resp.raise_for_status()
    return resp.json()


# ============================================
# 5) Build event schema for the raw data lake layer
# ============================================

def build_event(monitoring_ref: str, payload: dict) -> dict:
    """
    Build a raw event to send to Kafka, with a simple schema:

      - event_source: logical source of the event
      - event_version: schema version
      - producer_ts: UTC timestamp on the producer side
      - monitoring_ref: StopPoint SIRI ID
      - payload: full raw JSON from the API
    """
    now_utc = datetime.now(timezone.utc).isoformat()

    event = {
        "event_source": "idfm.stopmonitoring",
        "event_version": 1,
        "producer_ts": now_utc,
        "monitoring_ref": monitoring_ref,
        "payload": payload,
    }
    return event


# ============================================
# 6) Main batch producer
# ============================================

def main():
    print("[INFO] Using ROI_STOPS_CSV:", ROI_STOPS_CSV)
    print("[INFO] Kafka bootstrap   :", KAFKA_BOOTSTRAP)
    print("[INFO] Kafka topic       :", TOPIC)

    producer = create_producer()

    total = 0
    ok = 0
    failed = 0

    for monitoring_ref, row in iter_roi_monitoring_refs(ROI_STOPS_CSV):
        total += 1
        stop_name = row.get("name") or ""
        roi_station = row.get("roi_station") or ""

        print(f"\n[INFO] ===== Stop {total} =====")
        print(f"[INFO] roi_station   : {roi_station}")
        print(f"[INFO] stop_name     : {stop_name}")
        print(f"[INFO] monitoring_ref: {monitoring_ref}")

        try:
            payload = call_stop_monitoring(monitoring_ref)
        except Exception as e:
            failed += 1
            print(f"[ERROR] API failed for {monitoring_ref}: {e}")
            continue

        event = build_event(monitoring_ref, payload)

        try:
            # Use monitoring_ref as the key to keep records for the same stop
            # in the same partition.
            producer.send(TOPIC, key=monitoring_ref, value=event)
            ok += 1
            print(f"[INFO] Sent event to Kafka for {monitoring_ref}")
        except Exception as e:
            failed += 1
            print(f"[ERROR] Kafka send failed for {monitoring_ref}: {e}")
            continue

        # Small delay between API calls to avoid hammering the endpoint
        time.sleep(0.2)

    producer.flush()
    print("\n[INFO] ===== BATCH DONE =====")
    print(f"[INFO] Total stops   : {total}")
    print(f"[INFO] Success events: {ok}")
    print(f"[INFO] Failed        : {failed}")


if __name__ == "__main__":
    main()
