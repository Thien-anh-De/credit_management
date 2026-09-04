# 📊 Phase 4: Real-Time Analytics & Operations Dashboard

**Objective:** Build a Web Operations Monitoring Center displaying KPIs, real-time ingestion throughput, and real-time fraud transaction detection.

---

## 🛠️ Implementation Details & Tools:

### 1. Web Operations Dashboard (`src/web_dashboard.py`)
- **Framework:** Streamlit & Plotly (Running on port `http://localhost:8501`).
- **Modern Dark Mode Glassmorphism Interface:**
  - **Stream Status Banner:** Displays Spark Engine status (`🟢 ACTIVE`) and latest transaction timestamp ingested into the pipeline.
  - **5 Metric KPI Cards:**
    1. 👥 **User Profiles (`dim_users`):** 2,000
    2. 💳 **Credit Cards (`dim_cards`):** 6,146
    3. ⚡ **Total Transactions (`fact_transactions`):** 60,000+ (Auto-incrementing)
    4. 💵 **Total Ingestion Volume ($):** Aggregated transaction dollar amount
    5. 🚨 **Fraud Alerts:** Count & Percentage (%) of fraudulent transactions
  - **Line Chart (Real-Time Ingestion Speed):** Monitors transaction ingestion throughput rate per minute/second.
  - **Donut Chart (Payment Entry Method):** Breakdown of Chip vs. Swipe vs. Online transactions.
  - **Live Feed Alert Table:** Automatically **highlights fraudulent transaction rows in red (`is_fraud == True`)**, enabling operations personnel to take immediate action.
  - **Auto-Refresh Engine:** Queries updated Silver Parquet files every 3 seconds.

### 2. Deep-Dive Analytics with Jupyter Notebook (`notebooks/data_analysis.ipynb`)
- Directly reads 3 Silver Parquet tables from `data/silver/`.
- Executes complex PySpark SQL queries (e.g., age groups with highest fraud incidence rates, credit card limit vs. income distribution analysis).

---

## 💡 Achievements:
The system provides a visual analytics platform, enabling 24/7 real-time monitoring across all banking transaction streams.
