# IDFM Data Processing Pipeline (Kafka + Spark on YARN + HDFS + InfluxDB/Grafana)

This repository contains a distributed data pipeline for Île-de-France Mobilités (IDFM) Stop Monitoring data. The pipeline ingests events into Kafka, processes them using Spark on YARN, stores outputs in HDFS (raw and curated layers), and optionally pushes curated results and pipeline metrics into InfluxDB for visualization in Grafana.

---

## Project Title and Description

**Purpose**
- Collect Stop Monitoring events from IDFM APIs
- Ingest events into Kafka (buffering and decoupling)
- Process and clean data with Spark submitted to YARN (distributed execution)
- Store outputs in HDFS:
  - **Raw layer:** traceable ingestion output
  - **Curated layer:** cleaned/standardized layer used to build KPIs
- Optional: write curated results and pipeline metrics into InfluxDB for Grafana dashboards

**Technologies**
- Hadoop (HDFS, YARN)
- Spark (PySpark on YARN)
- Kafka + ZooKeeper
- InfluxDB + Grafana (optional)
- Python (producer/consumer + utility scripts)

---
## How ROI Stops/Lines were built from IDFM APIs and used in the pipeline

This project uses IDFM PRIM APIs to construct two deterministic reference inputs:
- `roi_stops.csv` (StopPoints/StopAreas to monitor)
- `roi_lines.csv` (Lines to keep in the curated layer)

The process below explains how these files were derived and how they are used end-to-end.

### Step 1 — Collect Stop Monitoring candidates from iCAR
Source: https://prim.iledefrance-mobilites.fr/en/apis/idfm-icar

1. Query the iCAR API to retrieve stop monitoring candidates (stops/stations).
2. Filter to the stations/stops of interest (Region of Interest, ROI), based on the project scope.

### Step 2 — Normalize IDs to build `roi_stops.csv`
Using the **iCAR sample format** and the returned payload structure:
- Extract stable stop identifiers (StopPoint / StopArea identifiers)
- Normalize them into a consistent format that can be used as inputs for stop-based queries
- Export the resulting list as `kafka-scripts/reference/roi_stops.csv`

This normalization ensures the stop identifiers in `roi_stops.csv` match what the real-time responses use (e.g., `MonitoringRef.value` fields).

### Step 3 — Retrieve Lines from iLico
Source: https://prim.iledefrance-mobilites.fr/en/apis/idfm-ilico

1. Query the iLico API to retrieve available lines and line metadata.
2. Filter to the set of lines relevant to the ROI or project topic.

### Step 4 — Cross-check with iCAR responses to build `roi_lines.csv`
Source: https://prim.iledefrance-mobilites.fr/en/apis/idfm-icar

To ensure consistent identifiers across APIs:
- Compare iLico line identifiers with the line references observed in iCAR responses
- Keep the stable IDs that match the real-time payload (e.g., those appearing in `LineRef.value`)
- Export the resulting mapping as `kafka-scripts/reference/roi_lines.csv`

### Step 5 — Call Stop Monitoring per stop using `roi_stops.csv` and write Raw
The producer reads the stop list from `roi_stops.csv` and calls the stop-based endpoint accordingly, producing:
- Kafka messages (one JSON event per record)
- A raw layer in HDFS written by the Spark job (Kafka -> HDFS raw)

> API quota note: Stop-based calls are limited to **1000 requests/day per access key**, so ROI filtering and throttling are required.

### Step 6 — Curated processing joins with `roi_lines.csv`
During the curated batch step:
- Parse and normalize the nested SIRI payload fields
- Join curated events with `roi_lines.csv` to keep only the lines of interest and/or enrich with line-level metadata
- Write the curated output for KPI computation and optional downstream sinks (InfluxDB/Grafana)

---
## Repository Structure

- `kafka-scripts/`
  - `stopmon_batch_producer.py` : API -> Kafka (Stop Monitoring)
  - `kafka_to_hdfs_raw.py` : Kafka -> HDFS raw (Spark on YARN)
  - `stopmonitoring_curated_batch.py` : HDFS raw -> curated (Spark batch)
  - `curated_to_influx_v3.py` : curated -> InfluxDB (Spark on YARN)
  - `producer_metrics.py` : metrics producer (Python)
  - `kafka_to_influx_metrics.py` : Kafka -> InfluxDB metrics consumer (Python)
  - `reference/roi_lines.csv`, `reference/roi_stops.csv` : reference mapping files (ROI)
- `configs/` : configuration snapshots used on the cluster
  - `configs/hadoop/` : `core-site.xml`, `hdfs-site.xml`, `yarn-site.xml`, `mapred-site.xml`, `workers`
  - `configs/spark/` : `spark-defaults.conf`, `spark-env.sh`, and templates (log4j/metrics/fair scheduler)
  - `configs/kafka/` : `server.properties`, `zookeeper.properties`
- `sample_data/`
  - `idfm_stop_monitoring_raw_sample.jsonl` : small sample dataset (30 Kafka messages exported)

---

## Cluster Setup Instructions

### List of nodes and roles
Typical deployment (based on this project):
- **master**
  - HDFS: NameNode
  - YARN: ResourceManager
  - Kafka broker (listener: `PLAINTEXT://master:9092`)
  - ZooKeeper (`master:2181`)
- **worker1**
  - HDFS: DataNode
  - YARN: NodeManager
  - Spark executors (confirmed in Spark UI -> Executors)

The exact configuration files are provided under `configs/` for reproducibility.

### How to start Hadoop (HDFS + YARN)
On **master**:
```bash
start-dfs.sh
start-yarn.sh
```

Quick verification (master):
```bash
hostname
jps
# Expect at least: NameNode, ResourceManager
```

Verification (worker1):
```bash
hostname
jps
# Expect at least: DataNode, NodeManager
```

### How to start Kafka (ZooKeeper + Broker)
On **master**:
```bash
cd /opt/kafka/kafka_2.13-3.7.2
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
```

Check topics:
```bash
/opt/kafka/kafka_2.13-3.7.2/bin/kafka-topics.sh --list --bootstrap-server master:9092
```

---

## How to Run Your Function (Pipeline Commands)

### 1) API -> Kafka (Producer)
Activate Python environment:
```bash
source /opt/spark/venv/bin/activate
```

Run the Stop Monitoring producer:
```bash
python3 kafka-scripts/stopmon_batch_producer.py
```

Show recent messages for demo (auto-exit, no hanging):
```bash
/opt/kafka/kafka_2.13-3.7.2/bin/kafka-console-consumer.sh \
  --bootstrap-server master:9092 \
  --topic idfm_stop_monitoring_raw \
  --group demo-$(date +%s) \
  --timeout-ms 10000 \
  --max-messages 10
```

### 2) Kafka -> HDFS Raw (Spark on YARN)
```bash
/opt/spark/bin/spark-submit \
  --master yarn \
  --deploy-mode client \
  --name kafka_to_hdfs_raw \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7,org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.7 \
  --num-executors 2 \
  --executor-cores 2 \
  --executor-memory 1g \
  --driver-memory 1g \
  kafka-scripts/kafka_to_hdfs_raw.py
```

### 3) HDFS Raw -> Curated (Spark Batch)
```bash
/opt/spark/bin/spark-submit \
  --master yarn \
  --deploy-mode client \
  --name raw_to_curated \
  --num-executors 2 \
  --executor-cores 1 \
  --executor-memory 1g \
  kafka-scripts/stopmonitoring_curated_batch.py
```

### 4) Curated -> InfluxDB (Optional)
```bash
/opt/spark/bin/spark-submit \
  --master yarn \
  --deploy-mode client \
  --name curated_to_influx \
  --num-executors 2 \
  --executor-cores 1 \
  --executor-memory 1g \
  kafka-scripts/curated_to_influx_v3.py
```

### 5) Metrics scripts (Python, NOT Spark)
These scripts are executed directly with Python (no `spark-submit`):
```bash
source /opt/spark/venv/bin/activate
python3 kafka-scripts/producer_metrics.py
python3 kafka-scripts/kafka_to_influx_metrics.py
```

---

## Dependencies

- Kafka: `kafka_2.13-3.7.2`
- Spark: installed under `/opt/spark`, submitted to YARN
- Spark Kafka packages:
  - `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7`
  - `org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.7`
- Hadoop: installed under `/usr/local/hadoop` (HDFS + YARN); config snapshots in `configs/hadoop/`
- Python: virtual environment at `/opt/spark/venv`
- Java: OpenJDK (version depends on VM image)

---

## Monitoring

### YARN ResourceManager UI
Access RM UI via SSH tunnel from your local machine:
```bash
ssh -L 8088:10.0.0.64:8088 adm-mcsc@<PUBLIC_MASTER_DNS>
```

Open:
- http://localhost:8088

### Spark UI (on YARN)
While a Spark job is running, open the YARN tracking URL / proxy, for example:
- http://localhost:8088/proxy/application_<APP_ID>/

To prove distributed execution:
- Spark UI -> **Executors** tab should show executors running on multiple hosts (e.g., `master` and `worker1`).

### Grafana (if enabled)
If Grafana is configured in your deployment, use it to show at least:
- Hadoop/YARN memory usage
Optionally:
- CPU utilization, running applications/jobs, resource usage

---

## Sample Data

- `sample_data/idfm_stop_monitoring_raw_sample.jsonl` contains 30 exported Stop Monitoring messages (JSON Lines) for quick testing.
- Reference mapping files:
  - `kafka-scripts/reference/roi_lines.csv`
  - `kafka-scripts/reference/roi_stops.csv`

---

## Notes / Limitations (RAM and scheduling)

- Cluster RAM is limited. Running multiple streaming jobs concurrently may cause instability or resource contention.
- To ensure stability, the pipeline is executed as manual batch steps (Spark jobs launched on demand).
- Cron / orchestration (automatic scheduling, retries, alerting) is not implemented; in production, a scheduler/orchestrator should be added.
- Additional time was required to understand the IDFM API payload semantics and design a correct schema; this step is essential to compute meaningful KPIs on the curated layer.

---

## Demo Video Link
https://youtu.be/voSNVdAbRLY
