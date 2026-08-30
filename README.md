# 🚲 BikeStream (Enterprise Edition)

BikeStream is a Real-time Spatio-Temporal Data Processing system designed entirely with a Microservices architecture and written **100% in Python**. The system is built to manage, monitor, and predict smart bike-sharing systems across multiple major cities.

## 🌟 Key Features

1. **Real-time Data Ingestion**: Fetch GBFS-compliant bike station data in real-time (Python Kafka Producer).
2. **Big Data Stream Processing**: Process tens of thousands of events per second using Apache Spark (PySpark Streaming).
3. **Time-Series Storage**: High-speed storage and analysis using TimescaleDB (PostgreSQL).
4. **Machine Learning Predictor**: Station status prediction model (XGBoost) served via FastAPI.
5. **Dynamic Dashboard (Python Streamlit)**: Modern dashboard interface featuring Glassmorphism, real-time mapping (Folium), and advanced charting (Plotly).
6. **Vector Maps & Routing**: Leverage spatial data via PostGIS, served by `pg_tileserv`, and compute bike fleet rebalancing routes via the OSRM API.

---

## 🛠️ Tech Stack (100% Python Architecture)

The system is standardized so Data Engineers / Data Scientists only need to use **Python**:
- **Data Ingestion**: Python (`confluent-kafka`, `requests`)
- **Data Processing**: PySpark (Python API for Apache Spark)
- **Machine Learning**: Python (`scikit-learn`, `xgboost`, `fastapi`)
- **Web Dashboard**: Python (`streamlit`, `streamlit-folium`, `plotly`, `psycopg2`)
- **Infrastructure**: Docker, Kafka, Zookeeper, Confluent Schema Registry, TimescaleDB, PostGIS

---

## 🚀 Setup & Execution

### 1. System Requirements
- **Docker** and **Docker Compose**
- Python 3.10+ (For local development)
- Minimum 8GB RAM

### 2. Start the System
The project uses Docker Compose to automatically spin up and network all containers.
```bash
docker-compose up -d
```

The services will be available at the following ports:
- **Dashboard (Streamlit)**: [http://localhost:8501](http://localhost:8501)
- **ML Predictor API (FastAPI)**: [http://localhost:8000/docs](http://localhost:8000/docs)
- **Vector Tiles (pg_tileserv)**: [http://localhost:7800](http://localhost:7800)
- **Spark UI**: [http://localhost:8080](http://localhost:8080)
- **TimescaleDB**: `localhost:5433` (Credentials: `bikestream` / `bikestream_2024`)

### 3. Stop the System
```bash
docker-compose down
```
*(Add the `-v` flag to completely wipe historical database data)*

---

## 📂 Project Structure

```
BikeStream/
├── docker-compose.yml       # Orchestrates all Microservices
├── db/                      # DB Init Scripts (PostGIS, TimescaleDB Schema)
├── producer/                # Python: Fetches GBFS data -> Kafka
├── spark/                   # PySpark: Reads Kafka -> Processes -> TimescaleDB
├── ml-predictor/            # Python: Prediction API (FastAPI + XGBoost)
├── dashboard/               # Python: Real-time Streamlit Dashboard
│   ├── app_realtime.py      # Core UI logic
│   └── Dockerfile           # Streamlit packaging
└── docs/                    # Documentation and Avro Schemas
```

---

## 💡 Restructuring Phases

- **Phase 1-6**: Deployed Kafka, PySpark, TimescaleDB, ML API, Vector Tiles.
- **Phase 7**: Integrated Confluent Schema Registry (Avro) & Dead Letter Queue (DLQ).
- **Phase 8-9**: Upgraded to a Premium Glassmorphism UI & **Completed full migration to a 100% Python architecture**.
