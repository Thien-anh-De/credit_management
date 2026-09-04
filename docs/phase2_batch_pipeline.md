# 📦 Phase 2: Batch Data Processing Pipeline

**Objective:** Ingest, clean, and standardize static dimension tables (Users & Credit Cards) from Bronze CSV raw format into Silver Parquet format.

---

## 🛠️ Implementation Details & Code Logic:

### 1. User Profiles Processing (`src/batch_jobs/process_users.py`)
- **Input:** `data/bronze/sd254_users.csv` (2,000 records).
- **Data Cleansing Steps:**
  1. Strip currency symbols (`$`) from income columns (`per_capita_income`, `yearly_income`, `total_debt`).
  2. Handle missing/null values with standard default values.
  3. Enforce strict schema data types (`IntegerType`, `DoubleType`).
  4. Implement cleanup helper logic using `shutil.rmtree` to prevent file locking issues on Windows OS.
- **Output:** Overwrite Snappy-compressed Parquet files into `data/silver/dim_users/`.

### 2. Credit Cards Processing (`src/batch_jobs/process_cards.py`)
- **Input:** `data/bronze/sd254_cards.csv` (6,146 records).
- **Data Cleansing Steps:**
  1. Remove `$` symbols from credit limit columns (`credit_limit`).
  2. Convert `YES/NO` string flags in `card_on_dark_web` to Boolean values (`True/False`).
  3. Cast schema data types for limits, card open dates, and PIN change years.
  4. Automatically clear target output directory prior to writing.
- **Output:** Overwrite Snappy-compressed Parquet files into `data/silver/dim_cards/`.

---

## 💡 Key Architectural Optimization:
To prevent Batch Jobs from queuing in `WAITING` state due to 24/7 streaming jobs consuming all available Spark Master Cluster resource slots, both Batch jobs are configured to run in **`local[*]`** mode.

As a result, both Batch jobs process and ingest 2,000 user profiles and 6,146 credit card records in under **3 seconds** before releasing compute resources.
