# 💳 Real-Time Credit Card Streaming & Fraud Monitoring Pipeline

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.1-E25A1C.svg)](https://spark.apache.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-Distributed-231F20.svg)](https://kafka.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED.svg)](https://www.docker.com/)
[![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B.svg)](https://streamlit.io/)
[![Architecture](https://img.shields.io/badge/Architecture-Medallion%20(Bronze%20%E2%9E%9E%20Silver)-00CC96.svg)]()

An end-to-end real-time Data Engineering system designed for simulating, processing, and monitoring banking credit card transactions on a 24/7 basis. Built on the **Medallion Architecture (Bronze ➔ Silver Data Lake)**, this pipeline combines **Apache Kafka** and **Spark Structured Streaming** to feed a live Web Operations Center for real-time throughput and fraud detection monitoring.

---

## 🏗️ System Architecture

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

## 🔥 Key Features

- **🔄 24/7 POS Transaction Simulator:** The `kafka-producer` container emulates point-of-sale (POS) terminal activity, continuously streaming live transactions with real-time timestamps into a Kafka topic with automatic 10-retry reconnection logic.
- **⚡ Real-Time Distributed Processing (Spark Structured Streaming):** The `spark-streaming-consumer` container listens to the Kafka topic, parses JSON payloads, cleans incoming data, and incrementally appends records to the Silver layer as **Snappy Compressed Parquet** files every 5-second micro-batch.
- **🛡️ Fault-Tolerant Checkpointing:** Leverages Spark offset checkpointing to guarantee **Exactly-Once Semantics** and eliminate data loss during pipeline restarts.
- **🖥️ 24/7 Web Operations Center (Streamlit Dashboard):** Features a sleek Glassmorphic Dark Mode interface displaying:
  - 5 High-level KPI cards (Total Users, Credit Cards, Total Transactions, Total Volume $, Fraud Alerts).
  - Spark Engine status bar & latest transaction timestamps.
  - Real-time ingestion speed line charts & payment entry method distribution (Chip vs. Swipe vs. Online).
  - Live Feed table with automatic red highlighting for high-risk fraud transactions.
- **📦 End-to-End Containerization:** Fully orchestrated via `docker-compose.yml`, spinning up Kafka, Zookeeper, Spark Cluster, Batch Jobs, Streaming Consumer, Producer, and the Web Dashboard seamlessly.

---

## 🛠️ Tech Stack

| Category | Technology / Library |
| :--- | :--- |
| **Messaging Broker** | Apache Kafka 3.x, Apache Zookeeper |
| **Processing Engine** | Apache Spark 3.5.1 (PySpark) |
| **Storage Format** | Apache Parquet (Snappy Compression) |
| **Containerization** | Docker, Docker Compose |
| **Web Dashboard** | Streamlit, Plotly, Pandas |
| **Language & Tools** | Python 3.10+, Jupyter Notebook |

---

## 📂 Repository Structure

```text
credit_management/
├── data/
│   ├── bronze/                  # Raw input CSV datasets (Ignored in Git)
│   └── silver/                  # Silver Data Lake layer (Parquet format)
│       ├── dim_users/           # User dimension table (2,000 records)
│       ├── dim_cards/           # Credit Card dimension table (6,146 records)
│       ├── fact_transactions/   # Streaming fact transaction table
│       └── checkpoints/         # Spark Offset Checkpoints
├── docs/                        # Detailed technical documentation
│   ├── architecture_and_repo.md
│   ├── phase1_infrastructure.md
│   ├── phase2_batch_pipeline.md
│   ├── phase3_streaming_pipeline.md
│   └── phase4_analytics.md
├── src/
│   ├── batch_jobs/              # PySpark batch transformation scripts
│   │   ├── process_users.py
│   │   └── process_cards.py
│   ├── streaming_jobs/          # Kafka producer & Spark streaming scripts
│   │   ├── kafka_producer.py
│   │   └── spark_consumer.py
│   └── web_dashboard.py         # Streamlit Operations Center Web App
├── notebooks/                   # Jupyter Notebooks for EDA
│   └── data_analysis.ipynb
├── docker-compose.yml           # Docker services orchestration
├── requirements.txt             # Python dependencies
└── README.md                    # Project documentation
```

---

## 🚀 Quick Start Guide

### 1. Prerequisites
- **Docker** & **Docker Desktop** installed (with Docker Compose support).

### 2. Clone Repository & Download Dataset
```bash
git clone https://github.com/Thien-anh-De/credit_management.git
cd credit_management
```

📥 **Download Raw Dataset (Bronze Layer)**:
Due to file size constraints, raw datasets are excluded from Git repository. Download the raw CSV files from:
👉 **[Google Drive Dataset Folder](https://drive.google.com/drive/folders/1vymvt8bZYZYNWhMuC_XnDR8N7PXDVoMc)**

Extract and place the downloaded CSV files inside `data/bronze/`:
- `sd254_users.csv`
- `sd254_cards.csv`
- `credit_card_transactions-ibm_v2.csv`
- `User0_credit_card_transactions.csv`

---

### 3. Launch the Full Pipeline (Docker Compose)

Execute a single command to launch all infrastructure services and automated pipeline jobs:

```bash
docker compose up -d
```

> 💡 **Automated Execution within Containers:**
> - `spark-batch-users` & `spark-batch-cards`: Automatically process raw Bronze data into Silver Data Lake Parquet format (`dim_users`, `dim_cards`).
> - `spark-streaming-consumer`: Automatically subscribes to Kafka topic and appends streaming micro-batches into `fact_transactions`.
> - `kafka-producer`: Automatically simulates live POS terminal transaction generation.
> - `web-dashboard`: Automatically boots up the Streamlit Web Operations Center.

---

### 4. Access Admin & Monitoring Interfaces

- 🌐 **Web Operations Dashboard:** [`http://localhost:8501`](http://localhost:8501)
- ⚡ **Spark Master UI:** [`http://localhost:8080`](http://localhost:8080)
- 👷 **Spark Worker UI:** [`http://localhost:8081`](http://localhost:8081)

---

## 📝 Technical Documentation
Detailed phase-by-phase implementation guides are available in the `docs/` folder:
- 📄 [Phase 1: Docker Infrastructure Setup](docs/phase1_infrastructure.md)
- 📄 [Phase 2: Batch Data Pipeline Processing](docs/phase2_batch_pipeline.md)
- 📄 [Phase 3: Real-Time Streaming Data Pipeline](docs/phase3_streaming_pipeline.md)
- 📄 [Phase 4: Real-Time Analytics & Operations Dashboard](docs/phase4_analytics.md)
