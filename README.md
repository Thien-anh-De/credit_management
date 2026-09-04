# 💳 Real-Time Credit Card Streaming & Fraud Monitoring Pipeline

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.1-E25A1C.svg)](https://spark.apache.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-Distributed-231F20.svg)](https://kafka.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED.svg)](https://www.docker.com/)
[![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B.svg)](https://streamlit.io/)
[![Architecture](https://img.shields.io/badge/Architecture-Medallion%20(Bronze%20%E2%9E%9E%20Silver)-00CC96.svg)]()

Hệ thống Data Engineering thời gian thực phục vụ giả lập, xử lý và giám sát luồng giao dịch thẻ tín dụng ngân hàng theo chu kỳ 24/7. Dự án ứng dụng kiến trúc **Medallion (Bronze ➔ Silver Data Lake)** kết hợp bộ đôi **Apache Kafka** và **Spark Structured Streaming**, cung cấp bảng điều khiển Web Operations Center giám sát gian lận và lưu lượng thời gian thực.

---

## 🏗️ Kiến trúc Hệ thống (System Architecture)

```text
                                [ IBM Credit Card Dataset (CSV) ]
                                                │
       ┌────────────────────────────────────────┴────────────────────────────────────────┐
       ▼                                                                                 ▼
[ Batch Pipeline ]                                                             [ 24/7 Streaming Pipeline ]
process_users.py & process_cards.py                                            kafka_producer.py (POS Simulator)
       │                                                                                 │
       ▼                                                                                 ▼
[ Silver Data Lake ]                                                           [ Apache Kafka Broker ]
(dim_users & dim_cards Parquet)                                                (transactions_topic)
       │                                                                                 │
       │                                                                                 ▼
       │                                                                   [ Spark Structured Streaming ]
       │                                                                   spark_consumer.py (Micro-batch 5s)
       │                                                                                 │
       │                                                                                 ▼
       └───────────────────────────────────┬─────────────────────────────────────────────┘
                                           ▼
                       [ Silver Data Lake: fact_transactions ]
                                (Snappy Parquet)
                                           │
       ┌───────────────────────────────────┴───────────────────────────────────┐
       ▼                                                                       ▼
[ Web Operations Center ]                                               [ Jupyter Notebook ]
Streamlit & Plotly (Port 8501)                                          Exploratory Data Analysis (EDA)
```

---

## 🔥 Tính năng Nổi bật (Key Features)

- ** Giả lập POS Quẹt thẻ 24/7 (Continuous Streaming Simulator):** `kafka_producer.py` đóng vai trò máy POS quẹt thẻ tại ngân hàng, liên tục bắn giao dịch kèm mốc thời gian thực tại vào Kafka Topic với cơ chế tự động reconnect 10 lần.
- **⚡ Xử lý Phân tán Thời gian thực (Spark Structured Streaming):** `spark_consumer.py` lắng nghe luồng Kafka, bóc tách JSON, làm sạch dữ liệu và ghi nối tiếp vào tầng Silver dưới dạng **Snappy Compressed Parquet** theo từng micro-batch 5 giây.
- **🛡️ Đảm bảo Không mất Dữ liệu (Fault-Tolerant Checkpointing):** Sử dụng Spark Checkpoint offset quản lý chính xác vị trí tin nhắn Kafka, đảm bảo chuẩn **Exactly-Once Semantics**.
- **🖥️ Web Operations Center 24/7 (Streamlit Dashboard):** Giao diện Modern Dark Mode chuẩn Glassmorphic hiển thị:
  - 5 Thẻ KPI Thống kê (Khách hàng, Thẻ tín dụng, Tổng số giao dịch, Doanh số $, Cảnh báo gian lận).
  - Thanh trạng thái Spark Engine & Mốc thời gian giao dịch mới nhất.
  - Biểu đồ đường Tốc độ nạp Real-Time & Biểu đồ cơ cấu quẹt thẻ (Chip vs Swipe vs Online).
  - Bảng Live Feed tự động tô đỏ dòng giao dịch nguy cơ gian lận.
- **📦 Tự động hóa Containerization:** Đóng gói toàn bộ hạ tầng Kafka, Zookeeper, Spark Cluster và Web App qua `docker-compose.yml`.

---

## 🛠️ Công nghệ Sử dụng (Tech Stack)

| Hạng mục | Công nghệ / Thư viện |
| :--- | :--- |
| **Messaging Broker** | Apache Kafka 3.x, Apache Zookeeper |
| **Processing Engine** | Apache Spark 3.5.1 (PySpark) |
| **Storage Format** | Apache Parquet (Snappy Compression) |
| **Containerization** | Docker, Docker Compose |
| **Web Dashboard** | Streamlit, Plotly, Pandas |
| **Language & Tools** | Python 3.10+, Jupyter Notebook |

---

## 📂 Cấu trúc Thư mục Repository

```text
credit_managerment/
├── data/
│   ├── bronze/                  # Dữ liệu thô ban đầu (Raw CSV)
│   └── silver/                  # Tầng Silver Data Lake (Format Parquet)
│       ├── dim_users/           # Bảng chiều Khách hàng (2,000 records)
│       ├── dim_cards/           # Bảng chiều Thẻ tín dụng (6,146 records)
│       ├── fact_transactions/   # Bảng sự kiện giao dịch Streaming
│       └── checkpoints/         # Spark Offset Checkpoints
├── docs/                        # Tài liệu hướng dẫn kỹ thuật chi tiết
│   ├── architecture_and_repo.md
│   ├── phase1_infrastructure.md
│   ├── phase2_batch_pipeline.md
│   ├── phase3_streaming_pipeline.md
│   └── phase4_analytics.md
├── src/
│   ├── batch_jobs/              # Code PySpark xử lý Batch
│   │   ├── process_users.py
│   │   └── process_cards.py
│   ├── streaming_jobs/          # Code Kafka & Spark Streaming
│   │   ├── kafka_producer.py
│   │   └── spark_consumer.py
│   └── web_dashboard.py         # App Web Dashboard Giám sát (Streamlit)
├── notebooks/                   # Jupyter Notebook phân tích EDA
│   └── data_analysis.ipynb
├── docker-compose.yml           # Khởi tạo cụm Docker Services
├── requirements.txt             # Thư viện Python phụ thuộc
└── README.md                    # Tài liệu hướng dẫn dự án
```

---

## 🚀 Hướng dẫn Khởi chạy Dự án (Quick Start)

### 1. Yêu cầu Tiền đề (Prerequisites)
- Đã cài đặt **Docker** & **Docker Desktop** (hoặc Docker Compose).
- Python 3.10 trở lên.

### 2. Tải Mã nguồn & Dữ liệu
```bash
git clone https://github.com/your-username/credit-card-streaming-pipeline.git
cd credit-card-streaming-pipeline
```
*Đảm bảo các file CSV thô nằm trong thư mục `data/bronze/`:*
- `sd254_users.csv`
- `sd254_cards.csv`
- `credit_card_transactions-ibm_v2.csv`

### 3. Bật Toàn bộ Hạ tầng Container
```bash
docker compose up -d
```

### 4. Truy cập các Giao diện Quản trị & Giám sát
- 🌐 **Web Operations Dashboard:** [`http://localhost:8501`](http://localhost:8501)
- ⚡ **Spark Master Cluster UI:** [`http://localhost:8080`](http://localhost:8080)
- 👷 **Spark Worker UI:** [`http://localhost:8081`](http://localhost:8081)

---

## 📝 Tài liệu Tham khảo Kỹ thuật (Documentation)
Chi tiết từng giai đoạn phát triển dự án được lưu tại thư mục `docs/`:
- [Phase 1: Khởi tạo Hạ tầng Docker](docs/phase1_infrastructure.md)
- [Phase 2: Xử lý Dữ liệu Lô Batch Pipeline](docs/phase2_batch_pipeline.md)
- [Phase 3: Xử lý Dữ liệu Luồng Streaming Pipeline](docs/phase3_streaming_pipeline.md)
- [Phase 4: Giám sát & Trực quan hóa Real-Time](docs/phase4_analytics.md)
