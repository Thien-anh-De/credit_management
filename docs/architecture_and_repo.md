# 🏗️ System Architecture & Repository Structure

The system is designed following a streamlined **Real-Time Data Lakehouse Architecture**, leveraging the distributed processing power of **Apache Spark** and the message broker capabilities of **Apache Kafka**.

---

## 1. Data Flow Architecture

```text
[ Raw CSV Datasets (Bronze Zone) ]
       │
       ├────► [ Process Users & Cards (Batch) ] ────────────► [ Silver Data Lake: dim_users & dim_cards (Parquet) ]
       │                                                                      │
       └────► [ Kafka Producer (24/7 POS Simulator) ]                        │
                     │                                                        │
                     ▼                                                        │
              [ Kafka Broker ]                                                │
                     │                                                        │
                     ▼                                                        │
          [ Spark Streaming Consumer ] ─────────────────────► [ Silver Data Lake: fact_transactions (Parquet) ]
                                                                              │
                                                                              ▼
                                                                [ Streamlit Web Dashboard (Port 8501) ]
                                                                [ Jupyter Notebook (Analytics & EDA) ]
```

### Core Components:
* **Bronze Zone (Data Source):** Stores 3 raw input CSV datasets (`users`, `cards`, `transactions`).
* **Ingestion & Messaging Layer:** 
  * **Batch Ingestion:** Processes static user profile and credit card dimension data.
  * **Streaming Ingestion:** `kafka_producer.py` simulates thousands of real-world POS card swipes, continuously streaming transaction messages 24/7 into Kafka topic `transactions_topic`.
* **Processing Engine (Apache Spark 3.5.1):**
  * **Batch Engine:** Cleans and transforms static raw data into Silver dimension tables (`dim_users`, `dim_cards`).
  * **Structured Streaming Engine:** Consumes Kafka messages in 5-second micro-batches, parses JSON payloads, cleans records, and appends them to `fact_transactions`.
* **Silver Zone (Data Lake):** Stores cleansed, optimized Snappy-compressed Parquet files for high storage efficiency and fast query performance.
* **Visualization & Analytics Layer:**
  * **Web Operations Center (`web_dashboard.py`):** Interactive web dashboard powered by Streamlit & Plotly, displaying 5 KPI metric cards, real-time ingestion throughput charts, card entry method breakdown, and real-time fraud alerts.
  * **Jupyter Notebook (`notebooks/data_analysis.ipynb`):** Utilizes PySpark for deep-dive exploratory data analysis (EDA) and ad-hoc SQL queries.

---

## 2. Repository Structure

```text
credit_management/
├── data/
│   ├── bronze/                  # Contains raw initial CSV files (Ignored in Git)
│   └── silver/                  # Silver Data Lake layer (Parquet format)
│       ├── dim_users/           # User dimension table (2,000 records)
│       ├── dim_cards/           # Credit Card dimension table (6,146 records)
│       ├── fact_transactions/   # Streaming 24/7 transaction fact table
│       └── checkpoints/         # Spark Streaming Offset Checkpoints directory
├── docs/                        # Phase-by-phase technical documentation (Phases 1-4)
├── src/
│   ├── batch_jobs/              # PySpark batch processing source code
│   │   ├── process_users.py
│   │   └── process_cards.py
│   ├── streaming_jobs/          # Kafka & Spark Streaming source code
│   │   ├── kafka_producer.py
│   │   └── spark_consumer.py
│   └── web_dashboard.py         # Streamlit 24/7 Web Operations Center App
├── notebooks/                   # Jupyter Notebooks for deep-dive EDA
│   └── data_analysis.ipynb
├── docker-compose.yml           # Docker Compose orchestration for Zookeeper, Kafka, Spark & Dashboard
└── requirements.txt             # Python package dependencies
```
