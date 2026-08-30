# BikeStream: Real-Time Bike-Share Analytics Pipeline

BikeStream is a scalable, real-time spatial-temporal data pipeline that monitors and analyzes city bike-sharing networks. It ingests live [GBFS (General Bikeshare Feed Specification)](https://github.com/MobilityData/gbfs) data from multiple cities, processes the streams in real-time, stores them in an optimized time-series database, and visualizes the state of the system on a live interactive dashboard.

## 🏗️ Architecture

The project is built using a modern, scalable open-source data stack:

1. **Kafka (Confluent)**: High-throughput message broker that buffers incoming raw API responses.
2. **Spark Structured Streaming**: Real-time ETL processor that cleans, flattens, and calculates metrics from Kafka topics.
3. **TimescaleDB (PostgreSQL)**: Time-series database that uses hypertables and continuous aggregates to handle high-frequency spatial-temporal data efficiently.
4. **R Shiny**: An interactive real-time operations dashboard to monitor system health, detect rebalancing needs, and visualize fleet distribution.
5. **Docker Compose**: Container orchestration that bundles the entire infrastructure into a reproducible environment.

### Project Phases (Stacked Workflow)

This project was built incrementally using a pipeline/stacked branch workflow:
- `phase-1-infrastructure`: Base Docker Compose setup (Kafka, Spark, DB).
- `phase-2-kafka-producer`: Python producer scraping GBFS APIs.
- `phase-3-spark-streaming`: Spark streaming job logic.
- `phase-4-database-schema`: TimescaleDB schemas and retention policies.
- `phase-5-realtime-dashboard`: R Shiny operations dashboard.
- `phase-6-historical-batch`: Historical spatial analysis pipeline using `targets` and `sf`/`s2`.

## 🚀 Quick Start

Ensure you have Docker and Docker Compose installed on your system.

### 1. Start the Infrastructure
Bring up all the services (Zookeeper, Kafka, TimescaleDB, Spark Master/Worker, Producer, Spark Streaming Job, and Dashboard):
```bash
docker compose up -d --build
```

### 2. Verify Services
Check the status of the containers:
```bash
docker compose ps
```
You can also monitor the real-time processing via the Spark Master UI at [http://localhost:8080](http://localhost:8080).

### 3. Access the Live Dashboard
Once the streaming job populates the database (wait about 1 minute for the first batches), open the operational dashboard at:
👉 **[http://localhost:3838](http://localhost:3838)**

## 📊 Dashboard Features
- **Live Map**: Real-time geographical status of every station. Colored pins indicate `EMPTY` (Critical), `LOW` (Warning), `HEALTHY` (Good), and `FULL` (Critical) statuses.
- **System KPIs**: High-level metrics for total bikes, docks, and stations requiring immediate rebalancing.
- **Health Composition**: Stacked area chart showing the real-time distribution of station health over the last 12 hours.
- **Utilization Heatmap**: A native plotly heatmap visualizing the busiest time-buckets per city.
- **Alerts Table**: Priority queue for dispatch teams showing stations that have been entirely empty or full over the last 5 minutes.

## 🗄️ Historical Batch Pipeline
For deep-dive historical analysis (e.g., spatial clustering, route planning), the project includes a `targets`-based reproducible pipeline located in `/historical/`. 
It utilizes `sf` and `s2` for fast spherical geometry calculations.

To run the batch pipeline locally (requires R and spatial libraries `libgdal-dev`, `cmake`, etc.):
```R
# Set working directory to /historical
setwd("historical")
targets::tar_make()
```

## 🧹 Cleanup
To stop all services and wipe the database volumes:
```bash
docker compose down -v
```

---
*Built with 💙 using Python, R, Kafka, Spark, and TimescaleDB.*
