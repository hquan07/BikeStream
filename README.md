# 🚲 BikeStream (Enterprise Edition)

BikeStream là một hệ thống Xử lý Dữ liệu Chuỗi thời gian và Không gian (Spatio-Temporal Data) theo thời gian thực, thiết kế hoàn toàn theo kiến trúc Microservices và được viết **100% bằng Python**. Hệ thống chuyên phục vụ bài toán quản lý, giám sát và dự đoán hệ thống xe đạp công cộng thông minh (Smart Bike-Sharing) tại nhiều thành phố lớn.

## 🌟 Tính năng nổi bật

1. **Thu thập dữ liệu Real-time**: Lấy dữ liệu trạm xe đạp chuẩn GBFS theo thời gian thực (Python Kafka Producer).
2. **Xử lý Luồng Dữ liệu Lớn (Big Data)**: Xử lý hàng vạn sự kiện mỗi giây bằng Apache Spark (PySpark Streaming).
3. **Lưu trữ Chuỗi thời gian (Time-series)**: Lưu trữ và phân tích dữ liệu siêu tốc bằng TimescaleDB (PostgreSQL).
4. **Machine Learning Predictor**: Mô hình dự đoán trạng thái trạm (XGBoost) phục vụ bằng FastAPI.
5. **Dynamic Dashboard (Python Streamlit)**: Giao diện bảng điều khiển (Dashboard) hiện đại với hiệu ứng Glassmorphism, Bản đồ thời gian thực (Folium) và phân tích biểu đồ (Plotly).
6. **Bản đồ Vector & Routing**: Khai thác dữ liệu không gian bằng PostGIS, phục vụ qua `pg_tileserv` và tính toán đường đi tái cân bằng xe (Fleet Routing) qua OSRM API.

---

## 🛠️ Tech Stack (100% Python Architecture)

Hệ thống được chuẩn hóa để Data Engineers / Data Scientists chỉ cần sử dụng **duy nhất Python**:
- **Data Ingestion**: Python (`confluent-kafka`, `requests`)
- **Data Processing**: PySpark (Python API for Apache Spark)
- **Machine Learning**: Python (`scikit-learn`, `xgboost`, `fastapi`)
- **Web Dashboard**: Python (`streamlit`, `streamlit-folium`, `plotly`, `psycopg2`)
- **Infrastructure**: Docker, Kafka, Zookeeper, Confluent Schema Registry, TimescaleDB, PostGIS

---

## 🚀 Hướng dẫn Cài đặt & Khởi chạy

### 1. Yêu cầu hệ thống
- **Docker** và **Docker Compose**
- Python 3.10+ (Nếu chạy cục bộ)
- Tối thiểu 8GB RAM

### 2. Khởi động toàn bộ hệ thống
Hệ thống sử dụng Docker Compose để tự động khởi tạo và liên kết các container.
```bash
docker-compose up -d
```

Các dịch vụ sẽ khởi chạy tại các cổng sau:
- **Dashboard (Streamlit)**: [http://localhost:8501](http://localhost:8501)
- **ML Predictor API (FastAPI)**: [http://localhost:8000/docs](http://localhost:8000/docs)
- **Vector Tiles (pg_tileserv)**: [http://localhost:7800](http://localhost:7800)
- **Spark UI**: [http://localhost:8080](http://localhost:8080)
- **TimescaleDB**: `localhost:5433` (Tài khoản: `bikestream` / `bikestream_2024`)

### 3. Tắt hệ thống
```bash
docker-compose down
```
*(Thêm cờ `-v` để xóa toàn bộ dữ liệu lịch sử database)*

---

## 📂 Cấu trúc Dự án

```
BikeStream/
├── docker-compose.yml       # Điều phối toàn bộ các Microservices
├── db/                      # Scripts khởi tạo DB (PostGIS, TimescaleDB Schema)
├── producer/                # Python: Lấy dữ liệu GBFS đẩy vào Kafka
├── spark/                   # PySpark: Đọc Kafka, xử lý, ghi vào TimescaleDB
├── ml-predictor/            # Python: API dự đoán (FastAPI + XGBoost)
├── dashboard/               # Python: Streamlit Dashboard thời gian thực
│   ├── app_realtime.py      # Mã nguồn chính của giao diện
│   └── Dockerfile           # Đóng gói Streamlit
└── docs/                    # Tài liệu và cấu trúc Avro Schema
```

---

## 💡 Các Giai đoạn Tái cấu trúc (Phases)

- **Phase 1-6**: Triển khai Kafka, PySpark, TimescaleDB, ML API, Vector Tiles.
- **Phase 7**: Tích hợp Confluent Schema Registry (Avro) & Dead Letter Queue (DLQ).
- **Phase 8-9**: Nâng cấp giao diện Premium (Glassmorphism) & **Hoàn tất chuyển đổi sang 100% kiến trúc Python**.
