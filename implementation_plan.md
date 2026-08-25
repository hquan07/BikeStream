# BikeStream — Real-Time Multi-City Bike-Share Analytics Platform

## Project Overview

A **production-grade data engineering portfolio project** that ingests real-time bike-share station data from 5 major US cities, streams it through Kafka, processes with Spark, stores in TimescaleDB, and visualises via a real-time Shiny dashboard — alongside a batch historical analysis pipeline.

**This is a brand new, standalone project.**

---

## Final Project Structure

```
BikeStream/
│
├── docker-compose.yml
├── .env
├── Makefile
├── README.md
│
├── producer/                          # Phase 2 — Kafka Producer
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── config.yaml
│   └── gbfs_producer.py
│
├── spark/                             # Phase 3 — Spark Processing
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── stream_processor.py
│   └── metrics_aggregator.py
│
├── db/                                # Phase 4 — Database
│   ├── 01_schema.sql
│   ├── 02_continuous_aggs.sql
│   └── 03_retention.sql
│
├── dashboard/                         # Phase 5 — Real-Time Dashboard
│   ├── app_realtime.R
│   └── www/
│       └── custom.css
│
├── historical/                        # Phase 6 — Batch Analysis
│   ├── _targets.R
│   └── scripts/
│       ├── 00_download_divvy.R
│       ├── 01_merge_clean.R
│       ├── 02_temporal_analysis.R
│       ├── 03_od_matrix_flow.R
│       ├── 04_hotspot_kde.R
│       ├── 05_network_analysis.R
│       └── 06_forecasting.R
│
└── docs/
    ├── architecture.png
    └── demo_recording.mp4
```

---

## Tech Stack

| Layer | Technology | Version | Purpose |
|-------|-----------|---------|---------|
| Message Broker | Apache Kafka | 7.7 (Confluent) | Event streaming |
| Coordination | Zookeeper | 7.7 (Confluent) | Kafka coordination |
| Stream Processing | Apache Spark | 3.5 | Structured Streaming |
| Time-Series DB | TimescaleDB | latest-pg16 | Station snapshots storage |
| Producer | Python | 3.11 | GBFS API polling → Kafka |
| Dashboard | R + Shiny | 4.x | Real-time & historical visualization |
| Orchestration | Docker Compose | v2 | Single-command deployment |
| Batch Pipeline | R targets + Arrow | - | Historical analysis |

---

## Data Sources — GBFS Feeds (5 Cities)

| City | System | ~Stations | `station_status` URL | `station_information` URL |
|------|--------|-----------|---------------------|--------------------------|
| Chicago | Divvy | 1,500 | `https://gbfs.lyft.com/gbfs/1.1/chi/en/station_status.json` | `https://gbfs.lyft.com/gbfs/1.1/chi/en/station_information.json` |
| New York | Citi Bike | 2,000 | `https://gbfs.citibikenyc.com/gbfs/2.3/en/station_status.json` | `https://gbfs.citibikenyc.com/gbfs/2.3/en/station_information.json` |
| San Francisco | Bay Wheels | 550 | `https://gbfs.lyft.com/gbfs/1.1/bay/en/station_status.json` | `https://gbfs.lyft.com/gbfs/1.1/bay/en/station_information.json` |
| Washington DC | Capital Bikeshare | 700 | `https://gbfs.lyft.com/gbfs/1.1/dca/en/station_status.json` | `https://gbfs.lyft.com/gbfs/1.1/dca/en/station_information.json` |
| Boston | Bluebikes | 450 | `https://gbfs.lyft.com/gbfs/1.1/bos/en/station_status.json` | `https://gbfs.lyft.com/gbfs/1.1/bos/en/station_information.json` |

**Total: ~5,200 stations → ~173 events/sec → ~15M rows/day**

---

# PHASE 1: Project Initialization & Docker Infrastructure

## Step 1.1 — Create project directory

```bash
mkdir -p ~/BikeStream/{producer,spark,db,dashboard/www,historical/scripts,docs}
cd ~/BikeStream
git init
```

## Step 1.2 — Create `.env`

```env
# File: .env
POSTGRES_USER=bikestream
POSTGRES_PASSWORD=bikestream_2024
POSTGRES_DB=bikestream
KAFKA_BROKER=kafka:9092
SPARK_MASTER=spark://spark-master:7077
```

## Step 1.3 — Create `docker-compose.yml`

```yaml
# File: docker-compose.yml
version: "3.9"

services:
  # --- Zookeeper (Kafka dependency) ---
  zookeeper:
    image: confluentinc/cp-zookeeper:7.7.0
    container_name: bs-zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"
    healthcheck:
      test: echo ruok | nc localhost 2181 | grep imok
      interval: 10s
      timeout: 5s
      retries: 5

  # --- Kafka Broker ---
  kafka:
    image: confluentinc/cp-kafka:7.7.0
    container_name: bs-kafka
    depends_on:
      zookeeper:
        condition: service_healthy
    ports:
      - "9092:9092"
      - "29092:29092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:29092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
    healthcheck:
      test: kafka-topics --bootstrap-server localhost:9092 --list
      interval: 15s
      timeout: 10s
      retries: 10

  # --- TimescaleDB (PostgreSQL + TimescaleDB extension) ---
  timescaledb:
    image: timescale/timescaledb:latest-pg16
    container_name: bs-timescaledb
    ports:
      - "5432:5432"
    environment:
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: ${POSTGRES_DB}
    volumes:
      - pgdata:/var/lib/postgresql/data
      - ./db:/docker-entrypoint-initdb.d
    healthcheck:
      test: pg_isready -U ${POSTGRES_USER}
      interval: 10s
      timeout: 5s
      retries: 5

  # --- Spark Master ---
  spark-master:
    image: bitnami/spark:3.5
    container_name: bs-spark-master
    ports:
      - "8080:8080"    # Spark UI
      - "7077:7077"    # Spark master port
    environment:
      SPARK_MODE: master
      SPARK_MASTER_HOST: spark-master

  # --- Spark Worker ---
  spark-worker:
    image: bitnami/spark:3.5
    container_name: bs-spark-worker
    depends_on:
      - spark-master
    environment:
      SPARK_MODE: worker
      SPARK_MASTER_URL: spark://spark-master:7077
      SPARK_WORKER_MEMORY: 2G
      SPARK_WORKER_CORES: 2

  # --- GBFS Producer (Python) ---
  producer:
    build: ./producer
    container_name: bs-producer
    depends_on:
      kafka:
        condition: service_healthy
    environment:
      KAFKA_BROKER: kafka:9092
    restart: unless-stopped

  # --- Spark Streaming Job ---
  spark-job:
    build: ./spark
    container_name: bs-spark-job
    depends_on:
      - spark-master
      - kafka
      - timescaledb
    environment:
      SPARK_MASTER: spark://spark-master:7077
      KAFKA_BROKER: kafka:9092
      POSTGRES_HOST: timescaledb
      POSTGRES_PORT: 5432
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: ${POSTGRES_DB}
    restart: unless-stopped

volumes:
  pgdata:
```

## Step 1.4 — Create `Makefile` (convenience commands)

```makefile
# File: Makefile
.PHONY: up down logs status clean

up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f

status:
	docker compose ps

# Watch Kafka topic
kafka-watch:
	docker exec bs-kafka kafka-console-consumer \
		--bootstrap-server localhost:9092 \
		--topic station_status --from-beginning --max-messages 10

# Check DB row counts
db-status:
	docker exec bs-timescaledb psql -U bikestream -d bikestream \
		-c "SELECT city, count(*), max(time) as latest FROM station_snapshots GROUP BY city ORDER BY city;"

clean:
	docker compose down -v --remove-orphans
```

## Step 1.5 — Verify

```bash
# Start only infra (no producer/spark yet)
docker compose up -d zookeeper kafka timescaledb spark-master spark-worker

# Check all services are healthy
docker compose ps

# Verify Kafka is ready
docker exec bs-kafka kafka-topics --bootstrap-server localhost:9092 --list

# Verify TimescaleDB is ready
docker exec bs-timescaledb psql -U bikestream -d bikestream -c "SELECT default_version FROM pg_available_extensions WHERE name = 'timescaledb';"
```

**✅ Deliverable: All 5 infrastructure services running and healthy.**

---

# PHASE 2: Kafka Producer (Python)

## Step 2.1 — `producer/requirements.txt`

```
requests==2.32.3
kafka-python==2.1.3
pyyaml==6.0.2
schedule==1.2.2
```

## Step 2.2 — `producer/config.yaml`

```yaml
# File: producer/config.yaml
poll_interval_seconds: 30

kafka:
  bootstrap_servers: ${KAFKA_BROKER:-kafka:9092}
  topic: station_status

cities:
  - name: chicago
    system: divvy
    status_url: https://gbfs.lyft.com/gbfs/1.1/chi/en/station_status.json
    info_url: https://gbfs.lyft.com/gbfs/1.1/chi/en/station_information.json

  - name: new_york
    system: citibike
    status_url: https://gbfs.citibikenyc.com/gbfs/2.3/en/station_status.json
    info_url: https://gbfs.citibikenyc.com/gbfs/2.3/en/station_information.json

  - name: san_francisco
    system: bay_wheels
    status_url: https://gbfs.lyft.com/gbfs/1.1/bay/en/station_status.json
    info_url: https://gbfs.lyft.com/gbfs/1.1/bay/en/station_information.json

  - name: washington_dc
    system: capital_bikeshare
    status_url: https://gbfs.lyft.com/gbfs/1.1/dca/en/station_status.json
    info_url: https://gbfs.lyft.com/gbfs/1.1/dca/en/station_information.json

  - name: boston
    system: bluebikes
    status_url: https://gbfs.lyft.com/gbfs/1.1/bos/en/station_status.json
    info_url: https://gbfs.lyft.com/gbfs/1.1/bos/en/station_information.json
```

## Step 2.3 — `producer/gbfs_producer.py`

```python
#!/usr/bin/env python3
"""
GBFS Producer — Polls 5 cities' bike-share APIs and publishes to Kafka.

Each message on topic 'station_status' is a JSON object with:
  city, station_id, timestamp, num_bikes_available, num_docks_available,
  num_ebikes_available, is_renting, is_returning, station_name, lat, lon, capacity
"""

import os
import json
import time
import logging
from datetime import datetime, timezone

import requests
import yaml
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# --- Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("gbfs-producer")

# --- Load Config ---
def load_config(path="config.yaml"):
    with open(path) as f:
        raw = f.read()
    # Substitute env vars
    for key, val in os.environ.items():
        raw = raw.replace(f"${{{key}}}", val)
        raw = raw.replace(f"${{{key}:-{val}}}", val)
    return yaml.safe_load(raw)

# --- Fetch station info (cached — changes rarely) ---
def fetch_station_info(url: str) -> dict:
    """Returns dict mapping station_id -> {name, lat, lon, capacity}"""
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        stations = data.get("data", {}).get("stations", [])
        lookup = {}
        for s in stations:
            sid = s.get("station_id", "")
            lookup[sid] = {
                "station_name": s.get("name", "Unknown"),
                "lat": s.get("lat", 0.0),
                "lon": s.get("lon", 0.0),
                "capacity": s.get("capacity", 0),
            }
        return lookup
    except Exception as e:
        logger.error(f"Failed to fetch station info from {url}: {e}")
        return {}

# --- Fetch station status and produce to Kafka ---
def poll_and_produce(producer: KafkaProducer, city: dict, info_cache: dict, topic: str):
    """Poll one city's station_status and produce each station as a Kafka message."""
    try:
        resp = requests.get(city["status_url"], timeout=30)
        resp.raise_for_status()
        data = resp.json()
        stations = data.get("data", {}).get("stations", [])
        now = datetime.now(timezone.utc).isoformat()
        count = 0

        for s in stations:
            sid = s.get("station_id", "")
            info = info_cache.get(sid, {})

            message = {
                "city": city["name"],
                "system": city["system"],
                "station_id": sid,
                "timestamp": now,
                "num_bikes_available": s.get("num_bikes_available", 0),
                "num_docks_available": s.get("num_docks_available", 0),
                "num_ebikes_available": s.get("num_ebikes_available", 0),
                "num_scooters_available": s.get("num_scooters_available", 0),
                "is_renting": bool(s.get("is_renting", 0)),
                "is_returning": bool(s.get("is_returning", 0)),
                "station_name": info.get("station_name", "Unknown"),
                "lat": info.get("lat", 0.0),
                "lon": info.get("lon", 0.0),
                "capacity": info.get("capacity", 0),
            }

            # Key = city:station_id for partition affinity
            key = f"{city['name']}:{sid}".encode("utf-8")
            producer.send(topic, key=key, value=message)
            count += 1

        producer.flush()
        logger.info(f"[{city['name']}] Produced {count} station records")

    except Exception as e:
        logger.error(f"[{city['name']}] Poll failed: {e}")

# --- Main Loop ---
def main():
    config = load_config()
    broker = config["kafka"]["bootstrap_servers"]
    topic = config["kafka"]["topic"]
    interval = config["poll_interval_seconds"]

    # Wait for Kafka to be ready
    producer = None
    for attempt in range(30):
        try:
            producer = KafkaProducer(
                bootstrap_servers=broker,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
            )
            logger.info(f"Connected to Kafka at {broker}")
            break
        except NoBrokersAvailable:
            logger.warning(f"Kafka not ready (attempt {attempt+1}/30), retrying in 5s...")
            time.sleep(5)

    if producer is None:
        logger.critical("Could not connect to Kafka after 30 attempts. Exiting.")
        return

    # Pre-fetch station info for all cities (refresh every 30 minutes)
    info_caches = {}
    last_info_refresh = 0

    logger.info(f"Starting producer loop: {len(config['cities'])} cities, every {interval}s")

    while True:
        loop_start = time.time()

        # Refresh station info every 30 minutes
        if time.time() - last_info_refresh > 1800:
            for city in config["cities"]:
                logger.info(f"[{city['name']}] Refreshing station info...")
                info_caches[city["name"]] = fetch_station_info(city["info_url"])
                logger.info(f"[{city['name']}] Cached {len(info_caches[city['name']])} stations")
            last_info_refresh = time.time()

        # Poll all cities
        for city in config["cities"]:
            poll_and_produce(producer, city, info_caches.get(city["name"], {}), topic)

        # Sleep for remaining interval
        elapsed = time.time() - loop_start
        sleep_time = max(0, interval - elapsed)
        logger.info(f"Cycle complete in {elapsed:.1f}s. Sleeping {sleep_time:.1f}s...")
        time.sleep(sleep_time)


if __name__ == "__main__":
    main()
```

## Step 2.4 — `producer/Dockerfile`

```dockerfile
# File: producer/Dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY config.yaml .
COPY gbfs_producer.py .

CMD ["python", "-u", "gbfs_producer.py"]
```

## Step 2.5 — Verify

```bash
# Build and start producer
docker compose up -d --build producer

# Watch Kafka messages arrive
docker exec bs-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic station_status \
  --from-beginning --max-messages 5

# Expected output: 5 JSON lines with city, station_id, bikes, docks, lat, lon...
```

**✅ Deliverable: Producer polling 5 cities every 30s, messages flowing into Kafka.**

---

# PHASE 3: Spark Structured Streaming

## Step 3.1 — `spark/requirements.txt`

```
pyspark==3.5.3
```

## Step 3.2 — `spark/stream_processor.py`

```python
#!/usr/bin/env python3
"""
Spark Structured Streaming job.
Reads from Kafka topic 'station_status', enriches with computed fields,
and writes to TimescaleDB in micro-batches every 10 seconds.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, when, lit, current_timestamp, to_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, BooleanType, TimestampType
)

# --- Config from env ---
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
PG_HOST      = os.getenv("POSTGRES_HOST", "timescaledb")
PG_PORT      = os.getenv("POSTGRES_PORT", "5432")
PG_USER      = os.getenv("POSTGRES_USER", "bikestream")
PG_PASSWORD  = os.getenv("POSTGRES_PASSWORD", "bikestream_2024")
PG_DB        = os.getenv("POSTGRES_DB", "bikestream")
PG_URL       = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"

# --- Kafka message schema ---
STATION_SCHEMA = StructType([
    StructField("city",                   StringType()),
    StructField("system",                 StringType()),
    StructField("station_id",             StringType()),
    StructField("timestamp",              StringType()),
    StructField("num_bikes_available",    IntegerType()),
    StructField("num_docks_available",    IntegerType()),
    StructField("num_ebikes_available",   IntegerType()),
    StructField("num_scooters_available", IntegerType()),
    StructField("is_renting",             BooleanType()),
    StructField("is_returning",           BooleanType()),
    StructField("station_name",           StringType()),
    StructField("lat",                    DoubleType()),
    StructField("lon",                    DoubleType()),
    StructField("capacity",              IntegerType()),
])


def write_to_timescaledb(batch_df, batch_id):
    """Write each micro-batch to TimescaleDB via JDBC."""
    if batch_df.count() == 0:
        return

    # Compute fill_ratio and status
    enriched = batch_df \
        .withColumn("time", to_timestamp(col("timestamp"))) \
        .withColumn("total_slots",
            col("num_bikes_available") + col("num_docks_available")) \
        .withColumn("fill_ratio",
            when(col("total_slots") > 0,
                 col("num_bikes_available") / col("total_slots"))
            .otherwise(lit(0.0))) \
        .withColumn("status",
            when(col("num_bikes_available") == 0, lit("EMPTY"))
            .when(col("fill_ratio") < 0.2,        lit("LOW"))
            .when(col("fill_ratio") > 0.9,         lit("HIGH"))
            .when(col("num_docks_available") == 0, lit("FULL"))
            .otherwise(lit("HEALTHY"))) \
        .withColumn("needs_rebalancing",
            (col("status") == "EMPTY") | (col("status") == "FULL")) \
        .select(
            "time", "city", "station_id", "station_name",
            "lat", "lon", "capacity",
            "num_bikes_available", "num_docks_available",
            "num_ebikes_available", "num_scooters_available",
            "fill_ratio", "status", "needs_rebalancing"
        )

    # Write to station_snapshots table
    enriched.write \
        .format("jdbc") \
        .option("url", PG_URL) \
        .option("dbtable", "station_snapshots") \
        .option("user", PG_USER) \
        .option("password", PG_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    print(f"[Batch {batch_id}] Wrote {enriched.count()} rows to TimescaleDB")


def main():
    spark = SparkSession.builder \
        .appName("BikeStream-Processor") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,"
                "org.postgresql:postgresql:42.7.3") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Read from Kafka
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", "station_status") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    # Parse JSON
    parsed = raw_stream \
        .select(from_json(
            col("value").cast("string"),
            STATION_SCHEMA
        ).alias("data")) \
        .select("data.*")

    # Write in micro-batches
    query = parsed.writeStream \
        .foreachBatch(write_to_timescaledb) \
        .trigger(processingTime="10 seconds") \
        .option("checkpointLocation", "/tmp/bikestream_checkpoint") \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    main()
```

## Step 3.3 — `spark/metrics_aggregator.py`

```python
#!/usr/bin/env python3
"""
Second Spark job: reads from Kafka, computes 5-minute windowed
system metrics per city, and writes to system_metrics table.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, window,
    count, sum as spark_sum, avg, when
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, BooleanType
)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
PG_HOST      = os.getenv("POSTGRES_HOST", "timescaledb")
PG_PORT      = os.getenv("POSTGRES_PORT", "5432")
PG_USER      = os.getenv("POSTGRES_USER", "bikestream")
PG_PASSWORD  = os.getenv("POSTGRES_PASSWORD", "bikestream_2024")
PG_DB        = os.getenv("POSTGRES_DB", "bikestream")
PG_URL       = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"

SCHEMA = StructType([
    StructField("city",                   StringType()),
    StructField("station_id",             StringType()),
    StructField("timestamp",              StringType()),
    StructField("num_bikes_available",    IntegerType()),
    StructField("num_docks_available",    IntegerType()),
    StructField("num_ebikes_available",   IntegerType()),
    StructField("num_scooters_available", IntegerType()),
    StructField("capacity",              IntegerType()),
])


def write_metrics(batch_df, batch_id):
    if batch_df.count() == 0:
        return

    batch_df.write \
        .format("jdbc") \
        .option("url", PG_URL) \
        .option("dbtable", "system_metrics") \
        .option("user", PG_USER) \
        .option("password", PG_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    print(f"[Metrics Batch {batch_id}] Wrote {batch_df.count()} aggregate rows")


def main():
    spark = SparkSession.builder \
        .appName("BikeStream-MetricsAggregator") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,"
                "org.postgresql:postgresql:42.7.3") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", "station_status") \
        .option("startingOffsets", "latest") \
        .load()

    parsed = raw \
        .select(from_json(col("value").cast("string"), SCHEMA).alias("d")) \
        .select("d.*") \
        .withColumn("event_time", to_timestamp(col("timestamp"))) \
        .withColumn("total_slots",
            col("num_bikes_available") + col("num_docks_available")) \
        .withColumn("fill_ratio",
            when(col("total_slots") > 0,
                 col("num_bikes_available") / col("total_slots"))
            .otherwise(0.0)) \
        .withColumn("is_empty", (col("num_bikes_available") == 0).cast("int")) \
        .withColumn("is_full",  (col("num_docks_available") == 0).cast("int"))

    # 5-minute tumbling window aggregation per city
    agg = parsed \
        .withWatermark("event_time", "1 minute") \
        .groupBy(
            window(col("event_time"), "5 minutes"),
            col("city")
        ) \
        .agg(
            spark_sum("num_bikes_available").alias("total_bikes"),
            spark_sum("num_docks_available").alias("total_docks"),
            spark_sum("num_ebikes_available").alias("total_ebikes"),
            avg("fill_ratio").alias("avg_fill_ratio"),
            spark_sum("is_empty").alias("num_empty_stations"),
            spark_sum("is_full").alias("num_full_stations"),
            count("*").alias("station_count"),
        ) \
        .withColumn("time", col("window.start")) \
        .withColumn("utilization_pct", col("avg_fill_ratio") * 100) \
        .select(
            "time", "city", "total_bikes", "total_docks", "total_ebikes",
            "avg_fill_ratio", "num_empty_stations", "num_full_stations",
            "utilization_pct"
        )

    query = agg.writeStream \
        .foreachBatch(write_metrics) \
        .trigger(processingTime="5 minutes") \
        .option("checkpointLocation", "/tmp/bikestream_metrics_checkpoint") \
        .outputMode("update") \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    main()
```

## Step 3.4 — `spark/Dockerfile`

```dockerfile
# File: spark/Dockerfile
FROM bitnami/spark:3.5

USER root
RUN pip install pyspark==3.5.3
USER 1001

COPY stream_processor.py /app/stream_processor.py
COPY metrics_aggregator.py /app/metrics_aggregator.py

CMD ["spark-submit", \
     "--master", "spark://spark-master:7077", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.postgresql:postgresql:42.7.3", \
     "/app/stream_processor.py"]
```

## Step 3.5 — Verify

```bash
# Build and start Spark job
docker compose up -d --build spark-job

# Check Spark UI
open http://localhost:8080

# Check TimescaleDB for incoming data (wait ~30 seconds)
make db-status
```

**✅ Deliverable: Spark consuming from Kafka, enriching, and writing to TimescaleDB.**

---

# PHASE 4: Database Schema (TimescaleDB)

## Step 4.1 — `db/01_schema.sql`

```sql
-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ==========================================
-- Dimension: Station master data
-- ==========================================
CREATE TABLE IF NOT EXISTS stations (
    station_id    TEXT NOT NULL,
    city          TEXT NOT NULL,
    station_name  TEXT,
    lat           DOUBLE PRECISION,
    lon           DOUBLE PRECISION,
    capacity      INTEGER,
    updated_at    TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (city, station_id)
);

-- ==========================================
-- Fact: Station snapshots (raw time-series)
-- ==========================================
CREATE TABLE IF NOT EXISTS station_snapshots (
    time                    TIMESTAMPTZ     NOT NULL,
    city                    TEXT            NOT NULL,
    station_id              TEXT            NOT NULL,
    station_name            TEXT,
    lat                     DOUBLE PRECISION,
    lon                     DOUBLE PRECISION,
    capacity                INTEGER,
    num_bikes_available     INTEGER,
    num_docks_available     INTEGER,
    num_ebikes_available    INTEGER,
    num_scooters_available  INTEGER,
    fill_ratio              REAL,
    status                  TEXT,
    needs_rebalancing       BOOLEAN
);

-- Convert to hypertable (auto-partitioned by time, 1 chunk per day)
SELECT create_hypertable('station_snapshots', by_range('time'),
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- Indexes for dashboard queries
CREATE INDEX IF NOT EXISTS idx_ss_city_time
    ON station_snapshots (city, time DESC);
CREATE INDEX IF NOT EXISTS idx_ss_station_time
    ON station_snapshots (station_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_ss_rebalancing
    ON station_snapshots (needs_rebalancing, city, time DESC)
    WHERE needs_rebalancing = TRUE;

-- ==========================================
-- Fact: System metrics (5-min aggregates)
-- ==========================================
CREATE TABLE IF NOT EXISTS system_metrics (
    time                TIMESTAMPTZ     NOT NULL,
    city                TEXT            NOT NULL,
    total_bikes         INTEGER,
    total_docks         INTEGER,
    total_ebikes        INTEGER,
    avg_fill_ratio      REAL,
    num_empty_stations  INTEGER,
    num_full_stations   INTEGER,
    utilization_pct     REAL
);

SELECT create_hypertable('system_metrics', by_range('time'),
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);
```

## Step 4.2 — `db/02_continuous_aggs.sql`

```sql
-- Hourly summary per city (auto-maintained by TimescaleDB)
CREATE MATERIALIZED VIEW IF NOT EXISTS hourly_city_summary
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS bucket,
    city,
    AVG(fill_ratio)             AS avg_fill,
    MAX(num_bikes_available)    AS max_bikes,
    MIN(num_bikes_available)    AS min_bikes,
    COUNT(DISTINCT station_id)  AS active_stations
FROM station_snapshots
GROUP BY bucket, city
WITH NO DATA;

-- Refresh policy: update every 30 minutes, look back 2 hours
SELECT add_continuous_aggregate_policy('hourly_city_summary',
    start_offset    => INTERVAL '2 hours',
    end_offset      => INTERVAL '30 minutes',
    schedule_interval => INTERVAL '30 minutes',
    if_not_exists   => TRUE
);
```

## Step 4.3 — `db/03_retention.sql`

```sql
-- Auto-delete raw snapshots older than 30 days
SELECT add_retention_policy('station_snapshots', INTERVAL '30 days',
    if_not_exists => TRUE);

-- Keep aggregated metrics for 1 year
SELECT add_retention_policy('system_metrics', INTERVAL '365 days',
    if_not_exists => TRUE);
```

## Step 4.4 — Verify

```bash
docker exec bs-timescaledb psql -U bikestream -d bikestream -c "\dt"
# Should show: stations, station_snapshots, system_metrics

docker exec bs-timescaledb psql -U bikestream -d bikestream -c "
  SELECT city, count(*), min(time), max(time)
  FROM station_snapshots GROUP BY city;"
```

**✅ Deliverable: 3 tables created as hypertables with auto-retention.**

---

# PHASE 5: Real-Time Shiny Dashboard

## Step 5.1 — Install R packages

```r
install.packages(c("shiny", "bslib", "bsicons", "DBI", "RPostgres",
                    "dplyr", "ggplot2", "plotly", "leaflet", "scales",
                    "lubridate"))
```

## Step 5.2 — `dashboard/app_realtime.R`

> [!NOTE]
> This is a full Shiny app that connects to TimescaleDB. The `reactiveTimer(30000)` drives auto-refresh every 30 seconds. All queries go directly to PostgreSQL (no local data loading).

**Tabs:**

| Tab | Content | DB Query |
|-----|---------|----------|
| Live Map | Leaflet map, all cities, color by status | `SELECT DISTINCT ON (city, station_id) ... FROM station_snapshots ORDER BY time DESC` |
| System KPIs | Value boxes + sparklines per city | `SELECT * FROM system_metrics WHERE time > now() - '2 hours'` |
| Alerts | Table of EMPTY/FULL stations | `SELECT ... FROM station_snapshots WHERE needs_rebalancing = TRUE AND time > now() - '5 minutes'` |
| Trends | Hourly utilization curves | `SELECT * FROM hourly_city_summary` |

**Key queries for the dashboard:**

```sql
-- Latest snapshot per station (for Live Map)
SELECT DISTINCT ON (city, station_id)
    city, station_id, station_name, lat, lon,
    num_bikes_available, num_docks_available, num_ebikes_available,
    fill_ratio, status, capacity, time
FROM station_snapshots
ORDER BY city, station_id, time DESC;

-- System KPIs (for value boxes)
SELECT city,
    SUM(num_bikes_available) as total_bikes,
    SUM(num_docks_available) as total_docks,
    SUM(num_ebikes_available) as total_ebikes,
    COUNT(*) as total_stations,
    SUM(CASE WHEN needs_rebalancing THEN 1 ELSE 0 END) as alert_count,
    AVG(fill_ratio) as avg_utilization
FROM station_snapshots
WHERE time > NOW() - INTERVAL '1 minute'
GROUP BY city;

-- Rebalancing alerts
SELECT city, station_name, status,
    num_bikes_available, num_docks_available, capacity,
    time as last_seen
FROM station_snapshots
WHERE needs_rebalancing = TRUE
  AND time > NOW() - INTERVAL '5 minutes'
ORDER BY time DESC;
```

## Step 5.3 — `dashboard/www/custom.css`

Use the same dark-mode premium CSS from the existing historical dashboard (Inter font, gradient cards, centered headers, etc).

## Step 5.4 — Verify

```r
shiny::runApp("dashboard/app_realtime.R")
# Map should show ~5,200 stations across 5 cities
# Value boxes should update every 30 seconds
# Alerts table should list empty/full stations
```

**✅ Deliverable: 4-tab real-time dashboard updating every 30 seconds from TimescaleDB.**

---

# PHASE 6: Historical Batch Pipeline (R)

Port the existing analysis pipeline from the current `Divvy-Spatial-Temporal-Analysis` project.

## What to copy

| Source (current project) | Destination (BikeStream) |
|-------------------------|-------------------------|
| `scripts/00_download_divvy.R` | `historical/scripts/00_download_divvy.R` |
| `scripts/01_merge_clean.R` | `historical/scripts/01_merge_clean.R` |
| `scripts/02_temporal_analysis.R` | `historical/scripts/02_temporal_analysis.R` |
| `scripts/03_od_matrix_flow.R` | `historical/scripts/03_od_matrix_flow.R` |
| `scripts/04_hotspot_kde_mapping.R` | `historical/scripts/04_hotspot_kde.R` |
| `scripts/05_network_analysis.R` | `historical/scripts/05_network_analysis.R` |
| `scripts/06_forecasting.R` | `historical/scripts/06_forecasting.R` |
| `_targets.R` | `historical/_targets.R` |
| `dashboard/app.R` | Integrate into unified dashboard |

## Modifications needed

1. Update all `base_dir` paths to point to `historical/` subdirectory
2. Update `_targets.R` to use `historical/scripts/` paths
3. Optionally add a **comparison tab** that overlays 2019 historical hourly patterns against real-time patterns from TimescaleDB

**✅ Deliverable: Historical pipeline runs independently inside `historical/` subdirectory.**

---

# Full Startup Sequence

```bash
# 1. Clone and enter project
cd ~/BikeStream

# 2. Start everything
docker compose up -d --build

# 3. Wait ~60 seconds for all services to initialize

# 4. Verify infrastructure
docker compose ps                    # All services "Up (healthy)"

# 5. Verify data flow
make kafka-watch                     # JSON messages flowing
make db-status                       # Row counts growing per city

# 6. Open Spark UI
open http://localhost:8080            # Stream jobs running

# 7. Launch dashboard
Rscript -e 'shiny::runApp("dashboard/app_realtime.R")'

# 8. Run historical pipeline (one-time)
cd historical && Rscript -e 'targets::tar_make()'
```

---

# Portfolio README Structure

Your `README.md` should include:

1. **Header** — Project name, one-line description, architecture diagram image
2. **Demo** — Embedded GIF/video of the live dashboard
3. **Architecture** — Mermaid diagram (copy from above)
4. **Tech Stack** — Table (copy from above)
5. **Quick Start** — `docker compose up -d` instructions
6. **Data Sources** — GBFS explanation + city table
7. **Project Structure** — Tree view (copy from above)
8. **Key Design Decisions** — Why Kafka? Why TimescaleDB? Why multi-city?
9. **Performance** — Throughput numbers (173 events/s, 15M rows/day)
10. **Future Work** — ML anomaly detection, Grafana alerts, more cities

---

## Verification Checklist

- [ ] `docker compose up -d` starts all 7 services
- [ ] Kafka topic `station_status` receiving messages from 5 cities
- [ ] Spark UI shows 2 active streaming jobs
- [ ] TimescaleDB `station_snapshots` growing by ~5,200 rows every 30s
- [ ] TimescaleDB `system_metrics` updating every 5 minutes
- [ ] Shiny Live Map shows ~5,200 colored markers
- [ ] Shiny Alerts table shows EMPTY/FULL stations
- [ ] Shiny KPIs auto-refresh every 30 seconds
- [ ] Historical `targets::tar_make()` completes successfully
- [ ] `docker compose down && docker compose up -d` recovers cleanly
