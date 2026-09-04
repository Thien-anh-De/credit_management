# 🚀 Phase 1: Infrastructure Initialization & Containerization

**Objective:** Set up the development environment, project directory structure, and containerize all system infrastructure using Docker Compose.

---

## 🛠️ Implemented Steps:

### 1. Repository Structure & Raw Data Setup (Bronze Zone)
- Initialized standard directory layout: `data/bronze`, `data/silver`, `src/batch_jobs`, `src/streaming_jobs`, `docs`.
- Placed 3 raw CSV datasets from IBM Credit Card Dataset into `data/bronze/`:
  - `sd254_users.csv`: User profiles dataset (2,000 users).
  - `sd254_cards.csv`: Credit cards dataset (6,146 cards).
  - `credit_card_transactions-ibm_v2.csv`: Large transaction events dataset.

### 2. Infrastructure Containerization (`docker-compose.yml`)
Provisioned independent, isolated services running inside Docker Containers:
- **Zookeeper (`zookeeper:2181`)**: Manages Kafka cluster state and metadata.
- **Kafka Broker (`kafka:9092`)**: High-throughput real-time message broker hosting `transactions_topic`.
- **Spark Cluster (`spark-master:7077` & `spark-worker`)**: Distributed Big Data processing compute engine.
- **Spark Streaming Consumer (`spark-streaming-consumer`)**: Background service running 24/7 to read Kafka streams and write Parquet data.
- **Web Dashboard (`web-dashboard`)**: Real-time monitoring center powered by Streamlit running on `http://localhost:8501`.

---

## 💡 Achievements:
The complete infrastructure stack launches synchronously with a single command:
```bash
docker compose up -d
```
