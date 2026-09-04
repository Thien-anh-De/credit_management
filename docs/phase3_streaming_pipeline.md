# ⚡ Phase 3: 24/7 POS Simulation & Real-Time Ingestion Pipeline

**Objective:** Build a 24/7 continuous real-time transaction ingestion and processing pipeline for credit card swipe events.

---

## 🛠️ Implementation Details & Streaming Architecture:

### 1. POS Transaction Streaming Generator (`src/streaming_jobs/kafka_producer.py`)
- **Role:** Simulates thousands of real-world point-of-sale (POS) credit card terminal transactions.
- **Workflow:**
  - Reads raw transaction records from Bronze CSV file `data/bronze/credit_card_transactions-ibm_v2.csv`.
  - Attaches real-time system timestamps (`datetime.now()`) to each transaction payload.
  - Serializes transaction data into JSON format and continuously streams messages to Kafka Topic `transactions_topic`.
  - Wrapped inside an infinite `while True` loop with **Automatic Kafka Reconnection (10 retry attempts)**, ensuring robust 24/7 operation.

### 2. Spark Structured Streaming Engine (`src/streaming_jobs/spark_consumer.py`)
- **Role:** Listens, cleanses, and persists streaming transaction data in real time.
- **Workflow:**
  - Subscribes to Kafka Topic `transactions_topic` using **Spark Structured Streaming**.
  - Parses raw JSON strings into an explicit schema (User ID, Card Index, Amount, Merchant, Is Fraud).
  - Cleans monetary values (`amount` column stripping `$`) and standardizes fraud indicator flags (`is_fraud`).
  - Incremental write operations in **Append Mode** every **5-second Micro-batch** into Silver Data Lake: `data/silver/fact_transactions/`.
  - Configured **Offset Checkpointing (`data/silver/checkpoints/`)** to track Kafka offsets, ensuring **Exactly-Once Semantics** without data loss or duplication.

---

## 💡 Achievements:
Real-time transaction stream runs continuously 24/7, ingesting tens of thousands of real-time transactions smoothly and reliably into the Silver Data Lake.
