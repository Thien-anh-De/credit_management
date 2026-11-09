"""
FastAPI Backend Server for Credit Management & DWH Platform
Cung cấp REST API hiệu năng cao cho giao diện React.js Dashboard.
Chạy: python src/api_server.py (mặc định port 8000)
"""

import os
import time
import sqlite3
import pandas as pd
from datetime import datetime
from typing import Optional
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn

app = FastAPI(title="Credit DWH API", version="1.0.0")

# Cấu hình CORS để React.js (Vite port 5173 / 3000) gọi được API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# Đường dẫn Data Lake & DWH
SILVER_USERS_PATH     = os.path.join(BASE_DIR, "data", "silver", "dim_users")
SILVER_CARDS_PATH     = os.path.join(BASE_DIR, "data", "silver", "dim_cards")
SILVER_FACT_PATH      = os.path.join(BASE_DIR, "data", "silver", "fact_transactions")

GOLD_FACT_PATH        = os.path.join(BASE_DIR, "data", "gold", "fact_transactions")
GOLD_MONTHLY_PATH     = os.path.join(BASE_DIR, "data", "gold", "agg_monthly_merchant_vol")
GOLD_QUARTERLY_PATH   = os.path.join(BASE_DIR, "data", "gold", "agg_quarterly_merchant_vol")
GOLD_DAILY_CARD_PATH  = os.path.join(BASE_DIR, "data", "gold", "agg_daily_card_fraud")
GOLD_DAILY_MERCH_PATH = os.path.join(BASE_DIR, "data", "gold", "agg_daily_merchant_vol")
GOLD_MERCHANT_PATH    = os.path.join(BASE_DIR, "data", "gold", "dim_merchant")
GOLD_DATE_PATH        = os.path.join(BASE_DIR, "data", "gold", "dim_date")


def get_sqlite_conn():
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    tables = [
        ("dim_date", GOLD_DATE_PATH),
        ("dim_merchant", GOLD_MERCHANT_PATH),
        ("dim_users", os.path.join(BASE_DIR, "data", "gold", "dim_users")),
        ("dim_cards", os.path.join(BASE_DIR, "data", "gold", "dim_cards")),
        ("fact_transactions", GOLD_FACT_PATH),
        ("agg_daily_merchant_vol", GOLD_DAILY_MERCH_PATH),
        ("agg_monthly_merchant_vol", GOLD_MONTHLY_PATH),
        ("agg_quarterly_merchant_vol", GOLD_QUARTERLY_PATH),
        ("agg_daily_card_fraud", GOLD_DAILY_CARD_PATH)
    ]
    for view_name, path in tables:
        if os.path.exists(path):
            try:
                df = pd.read_parquet(path)
                df.to_sql(view_name, conn, index=False, if_exists="replace")
            except Exception:
                pass
    return conn

# SQLite connection singleton
_sqlite_conn = None

def get_db():
    global _sqlite_conn
    if _sqlite_conn is None:
        _sqlite_conn = get_sqlite_conn()
    return _sqlite_conn


@app.get("/api/kpi/streaming")
def get_streaming_kpi():
    users_count = 0
    cards_count = 0
    total_tx = 0
    total_amount = 0.0
    fraud_count = 0
    latest_tx_time = "Chưa có dữ liệu"
    is_active = False

    if os.path.exists(SILVER_USERS_PATH):
        try: users_count = len(pd.read_parquet(SILVER_USERS_PATH))
        except Exception: pass

    if os.path.exists(SILVER_CARDS_PATH):
        try: cards_count = len(pd.read_parquet(SILVER_CARDS_PATH))
        except Exception: pass

    if os.path.exists(SILVER_FACT_PATH):
        try:
            df = pd.read_parquet(SILVER_FACT_PATH)
            total_tx = len(df)
            if not df.empty:
                if "amount" in df.columns:
                    total_amount = float(df["amount"].sum())
                if "is_fraud" in df.columns:
                    fraud_count = int(df["is_fraud"].sum())
                if "transaction_timestamp" in df.columns:
                    max_ts = pd.to_datetime(df["transaction_timestamp"]).max()
                    latest_tx_time = max_ts.strftime('%H:%M:%S (%Y-%m-%d)')
                    is_active = True
        except Exception:
            pass

    fraud_rate = (fraud_count / total_tx * 100) if total_tx > 0 else 0.0

    return {
        "total_users": users_count,
        "total_cards": cards_count,
        "total_transactions": total_tx,
        "total_volume": round(total_amount, 2),
        "fraud_count": fraud_count,
        "fraud_rate": round(fraud_rate, 2),
        "latest_tx_time": latest_tx_time,
        "is_active": is_active
    }


@app.get("/api/streaming/live-feed")
def get_streaming_live_feed():
    recent = []
    timeline = []
    chip_dist = []

    if os.path.exists(SILVER_FACT_PATH):
        try:
            df = pd.read_parquet(SILVER_FACT_PATH)
            if not df.empty and "transaction_timestamp" in df.columns:
                # 15 giao dịch mới nhất
                cols = ["transaction_timestamp", "user_id", "card_index", "amount", "merchant_city", "merchant_state", "use_chip", "is_fraud"]
                avail_cols = [c for c in cols if c in df.columns]
                recent_df = df.sort_values(by="transaction_timestamp", ascending=False).head(15)[avail_cols]
                # convert timestamp to str
                if "transaction_timestamp" in recent_df.columns:
                    recent_df["transaction_timestamp"] = recent_df["transaction_timestamp"].astype(str)
                recent = recent_df.to_dict(orient="records")

                # Timeline theo phút
                df["ts_min"] = pd.to_datetime(df["transaction_timestamp"]).dt.strftime('%H:%M')
                t_df = df.groupby("ts_min").size().reset_index(name="count").tail(15)
                timeline = t_df.to_dict(orient="records")

                # Hình thức quẹt thẻ
                if "use_chip" in df.columns:
                    c_df = df["use_chip"].value_counts().reset_index()
                    c_df.columns = ["name", "value"]
                    chip_dist = c_df.to_dict(orient="records")
        except Exception as e:
            print(f"Error reading live feed: {e}")

    return {
        "recent_transactions": recent,
        "timeline": timeline,
        "chip_distribution": chip_dist
    }


@app.get("/api/kpi/gold")
def get_gold_kpi():
    total_volume = 0.0
    total_tx = 0
    fraud_count = 0
    total_merchants = 0

    if os.path.exists(GOLD_MONTHLY_PATH):
        try:
            df = pd.read_parquet(GOLD_MONTHLY_PATH)
            if not df.empty:
                if "total_volume" in df.columns:
                    total_volume = float(df["total_volume"].sum())
                if "total_txn" in df.columns:
                    total_tx = int(df["total_txn"].sum())
                if "fraud_count" in df.columns:
                    fraud_count = int(df["fraud_count"].sum())
                if "merchant_sk" in df.columns:
                    total_merchants = int(df["merchant_sk"].nunique())
        except Exception:
            pass

    return {
        "total_volume": round(total_volume, 2),
        "total_transactions": total_tx,
        "fraud_count": fraud_count,
        "total_merchants": total_merchants
    }


@app.get("/api/gold/monthly-trends")
def get_gold_monthly_trends():
    trends = []
    if os.path.exists(GOLD_MONTHLY_PATH):
        try:
            df = pd.read_parquet(GOLD_MONTHLY_PATH)
            if not df.empty and "year" in df.columns and "month_number" in df.columns:
                m = df.groupby(["year", "month_number"]).agg({
                    "total_volume": "sum",
                    "total_txn": "sum",
                    "fraud_count": "sum"
                }).reset_index()
                m["period"] = m["year"].astype(str) + "-" + m["month_number"].astype(str).str.zfill(2)
                m = m.sort_values(by=["year", "month_number"]).tail(18)
                m["total_volume"] = m["total_volume"].round(2)
                trends = m[["period", "total_volume", "total_txn", "fraud_count"]].to_dict(orient="records")
        except Exception:
            pass
    return trends


@app.get("/api/gold/quarterly-trends")
def get_gold_quarterly_trends():
    trends = []
    if os.path.exists(GOLD_QUARTERLY_PATH):
        try:
            df = pd.read_parquet(GOLD_QUARTERLY_PATH)
            if not df.empty and "year" in df.columns and "quarter" in df.columns:
                q = df.groupby(["year", "quarter"]).agg({
                    "total_volume": "sum",
                    "total_txn": "sum",
                    "fraud_count": "sum"
                }).reset_index()
                q["period"] = q["year"].astype(str) + " Q" + q["quarter"].astype(str)
                q = q.sort_values(by=["year", "quarter"]).tail(12)
                q["total_volume"] = q["total_volume"].round(2)
                trends = q[["period", "total_volume", "total_txn", "fraud_count"]].to_dict(orient="records")
        except Exception:
            pass
    return trends


@app.get("/api/gold/card-fraud-risks")
def get_gold_card_fraud():
    risks = []
    if os.path.exists(GOLD_DAILY_CARD_PATH):
        try:
            df = pd.read_parquet(GOLD_DAILY_CARD_PATH)
            if not df.empty and "fraud_count" in df.columns:
                f_df = df[df["fraud_count"] > 0].sort_values(by="fraud_count", ascending=False).head(10)
                risks = f_df.to_dict(orient="records")
        except Exception:
            pass
    return risks


@app.get("/api/inspector/tables")
def get_inspector_tables():
    table_map = {
        "fact_transactions": GOLD_FACT_PATH,
        "dim_cards": os.path.join(BASE_DIR, "data", "gold", "dim_cards"),
        "dim_users": os.path.join(BASE_DIR, "data", "gold", "dim_users"),
        "dim_merchant": GOLD_MERCHANT_PATH,
        "dim_date": GOLD_DATE_PATH,
        "agg_monthly_merchant_vol": GOLD_MONTHLY_PATH,
        "agg_quarterly_merchant_vol": GOLD_QUARTERLY_PATH,
        "agg_daily_card_fraud": GOLD_DAILY_CARD_PATH,
        "agg_daily_merchant_vol": GOLD_DAILY_MERCH_PATH
    }
    result = []
    for name, path in table_map.items():
        count = 0
        exists = os.path.exists(path)
        if exists:
            try:
                count = len(pd.read_parquet(path))
            except Exception:
                pass
        result.append({"table_name": name, "row_count": count, "available": exists})
    return result


@app.get("/api/inspector/table/{table_name}")
def get_inspector_table_data(table_name: str, limit: int = 25):
    path = os.path.join(BASE_DIR, "data", "gold", table_name)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Table not found")
    try:
        df = pd.read_parquet(path)
        total_rows = len(df)
        sub_df = df.head(limit)
        # Convert timestamp or dates to str
        for col in sub_df.columns:
            if "date" in col or "timestamp" in col:
                sub_df[col] = sub_df[col].astype(str)
        return {
            "table_name": table_name,
            "total_rows": total_rows,
            "columns": list(sub_df.columns),
            "rows": sub_df.to_dict(orient="records")
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


SCHEMA_METADATA = [
    {
        "table_name": "fact_transactions",
        "category": "Fact",
        "color": "#EF4444",
        "description": "Bảng sự kiện giao dịch quẹt thẻ Atomic Grain (1 dòng = 1 lượt quẹt)",
        "columns": [
            {"name": "transaction_id", "type": "VARCHAR(32)", "key": "PK", "desc": "Khóa suy biến băm MD5"},
            {"name": "date_sk", "type": "INT", "key": "FK", "ref": "dim_date.date_sk"},
            {"name": "user_sk", "type": "BIGINT", "key": "FK", "ref": "dim_users.user_sk (PIT)"},
            {"name": "card_sk", "type": "BIGINT", "key": "FK", "ref": "dim_cards.card_sk (PIT)"},
            {"name": "merchant_sk", "type": "BIGINT", "key": "FK", "ref": "dim_merchant.merchant_sk"},
            {"name": "amount", "type": "FLOAT", "key": "", "desc": "Số tiền quẹt thẻ ($)"},
            {"name": "is_fraud", "type": "BIT", "key": "", "desc": "1: Gian lận, 0: Hợp lệ"},
            {"name": "use_chip", "type": "VARCHAR(30)", "key": "", "desc": "Swipe / Chip / Online"},
            {"name": "transaction_timestamp", "type": "DATETIME", "key": "", "desc": "Thời điểm POS"}
        ]
    },
    {
        "table_name": "dim_date",
        "category": "Dimension",
        "color": "#A855F7",
        "description": "Chiều thời gian 1995–2035 (SCD Type 0)",
        "columns": [
            {"name": "date_sk", "type": "INT", "key": "PK", "desc": "Khóa ngày YYYYMMDD"},
            {"name": "full_date_str", "type": "VARCHAR(10)", "key": "", "desc": "YYYY-MM-DD"},
            {"name": "year", "type": "INT", "key": "", "desc": "1995 - 2035"},
            {"name": "quarter", "type": "INT", "key": "", "desc": "Quý 1 - 4"},
            {"name": "month_number", "type": "INT", "key": "", "desc": "Tháng 1 - 12"},
            {"name": "month_name", "type": "VARCHAR(15)", "key": "", "desc": "Tên tháng"},
            {"name": "day_of_month", "type": "INT", "key": "", "desc": "Ngày trong tháng 1 - 31"},
            {"name": "day_of_week", "type": "INT", "key": "", "desc": "1 (CN) - 7 (Thứ 7)"},
            {"name": "day_name", "type": "VARCHAR(15)", "key": "", "desc": "Monday, Tuesday..."},
            {"name": "week_of_year", "type": "INT", "key": "", "desc": "Tuần trong năm 1 - 53"},
            {"name": "is_weekend", "type": "BIT", "key": "", "desc": "1: Cuối tuần, 0: Thường"},
            {"name": "is_holiday_vn", "type": "BIT", "key": "", "desc": "1: Ngày nghỉ lễ, 0: Thường"}
        ]
    },
    {
        "table_name": "dim_users",
        "category": "Dimension",
        "color": "#06B6D4",
        "description": "Chiều thông tin khách hàng (SCD Type 2)",
        "columns": [
            {"name": "user_sk", "type": "BIGINT", "key": "PK", "desc": "Surrogate Key phiên bản"},
            {"name": "user_id", "type": "BIGINT", "key": "NK", "desc": "Mã tự nhiên khách hàng"},
            {"name": "user_name", "type": "VARCHAR(100)", "key": "", "desc": "Họ và tên"},
            {"name": "current_age", "type": "INT", "key": "", "desc": "Tuổi hiện tại"},
            {"name": "yearly_income", "type": "FLOAT", "key": "", "desc": "Thu nhập năm ($)"},
            {"name": "total_debt", "type": "FLOAT", "key": "", "desc": "Tổng nợ ($)"},
            {"name": "fico_score", "type": "INT", "key": "", "desc": "Điểm tín dụng FICO"},
            {"name": "city", "type": "VARCHAR(100)", "key": "", "desc": "Thành phố"},
            {"name": "state", "type": "VARCHAR(50)", "key": "", "desc": "Bang / Tỉnh"},
            {"name": "effective_date", "type": "DATE", "key": "PIT", "desc": "Hiệu lực từ ngày"},
            {"name": "expiration_date", "type": "DATE", "key": "PIT", "desc": "Hết hiệu lực ngày"},
            {"name": "is_current", "type": "BIT", "key": "", "desc": "1: Bản ghi hiện hành"}
        ]
    },
    {
        "table_name": "dim_cards",
        "category": "Dimension",
        "color": "#F59E0B",
        "description": "Chiều thẻ tín dụng & rò rỉ Dark Web (SCD Type 2)",
        "columns": [
            {"name": "card_sk", "type": "BIGINT", "key": "PK", "desc": "Surrogate Key phiên bản thẻ"},
            {"name": "card_natural_key", "type": "VARCHAR(50)", "key": "NK", "desc": "user_id + card_index"},
            {"name": "user_id", "type": "BIGINT", "key": "FK", "ref": "dim_users.user_id"},
            {"name": "card_index", "type": "INT", "key": "", "desc": "Thứ tự thẻ"},
            {"name": "card_brand", "type": "VARCHAR(50)", "key": "", "desc": "Visa / Mastercard / Amex"},
            {"name": "card_type", "type": "VARCHAR(50)", "key": "", "desc": "Credit / Debit / Prepaid"},
            {"name": "credit_limit", "type": "FLOAT", "key": "", "desc": "Hạn mức tín dụng ($)"},
            {"name": "card_on_dark_web", "type": "BIT", "key": "", "desc": "1: Cảnh báo lộ Dark Web"},
            {"name": "acct_open_date", "type": "DATE", "key": "", "desc": "Ngày mở thẻ"},
            {"name": "effective_date", "type": "DATE", "key": "PIT", "desc": "Bắt đầu hiệu lực"},
            {"name": "expiration_date", "type": "DATE", "key": "PIT", "desc": "Kết thúc hiệu lực"},
            {"name": "is_current", "type": "BIT", "key": "", "desc": "1: Thẻ đang dùng"}
        ]
    },
    {
        "table_name": "dim_merchant",
        "category": "Dimension",
        "color": "#10B981",
        "description": "Chiều điểm bán POS & Thương nhân (SCD Type 1)",
        "columns": [
            {"name": "merchant_sk", "type": "BIGINT", "key": "PK", "desc": "Surrogate Key POS"},
            {"name": "merchant_name", "type": "VARCHAR(100)", "key": "", "desc": "Tên điểm bán / Cửa hàng"},
            {"name": "merchant_city", "type": "VARCHAR(100)", "key": "", "desc": "Thành phố đặt POS"},
            {"name": "merchant_state", "type": "VARCHAR(50)", "key": "", "desc": "Bang / Khu vực"},
            {"name": "mcc", "type": "VARCHAR(10)", "key": "", "desc": "Mã ngành hàng (MCC)"}
        ]
    },
    {
        "table_name": "agg_daily_merchant_vol",
        "category": "Summary",
        "color": "#EC4899",
        "description": "Tổng hợp doanh số Ngày x Merchant (Inmon Tầng Cơ Sở)",
        "columns": [
            {"name": "date_sk", "type": "INT", "key": "PK,FK", "ref": "dim_date.date_sk"},
            {"name": "merchant_sk", "type": "BIGINT", "key": "PK,FK", "ref": "dim_merchant.merchant_sk"},
            {"name": "total_txn", "type": "INT", "key": "", "desc": "Tổng lượt quẹt"},
            {"name": "total_volume", "type": "FLOAT", "key": "", "desc": "Tổng doanh số ($)"},
            {"name": "fraud_count", "type": "INT", "key": "", "desc": "Số vụ gian lận"},
            {"name": "avg_txn_amount", "type": "FLOAT", "key": "", "desc": "Giá trị trung bình/GD"},
            {"name": "fraud_rate_pct", "type": "FLOAT", "key": "", "desc": "Tỷ lệ gian lận (%)"}
        ]
    },
    {
        "table_name": "agg_monthly_merchant_vol",
        "category": "Summary",
        "color": "#EC4899",
        "description": "Tổng hợp doanh số Tháng x Merchant (Inmon Tầng Giữa)",
        "columns": [
            {"name": "year", "type": "INT", "key": "PK", "desc": "Năm tổng hợp"},
            {"name": "month_number", "type": "INT", "key": "PK", "desc": "Tháng 1 - 12"},
            {"name": "merchant_sk", "type": "BIGINT", "key": "PK,FK", "ref": "dim_merchant.merchant_sk"},
            {"name": "total_txn", "type": "INT", "key": "", "desc": "Tổng lượt quẹt tháng"},
            {"name": "total_volume", "type": "FLOAT", "key": "", "desc": "Tổng doanh số tháng ($)"},
            {"name": "fraud_count", "type": "INT", "key": "", "desc": "Số vụ gian lận tháng"},
            {"name": "avg_txn_amount", "type": "FLOAT", "key": "", "desc": "Giá trị trung bình/GD"},
            {"name": "fraud_rate_pct", "type": "FLOAT", "key": "", "desc": "Tỷ lệ gian lận (%)"}
        ]
    },
    {
        "table_name": "agg_quarterly_merchant_vol",
        "category": "Summary",
        "color": "#EC4899",
        "description": "Tổng hợp doanh số Quý x Merchant (Inmon Đỉnh Tháp)",
        "columns": [
            {"name": "year", "type": "INT", "key": "PK", "desc": "Năm tổng hợp"},
            {"name": "quarter", "type": "INT", "key": "PK", "desc": "Quý 1 - 4"},
            {"name": "merchant_sk", "type": "BIGINT", "key": "PK,FK", "ref": "dim_merchant.merchant_sk"},
            {"name": "total_txn", "type": "INT", "key": "", "desc": "Tổng lượt quẹt quý"},
            {"name": "total_volume", "type": "FLOAT", "key": "", "desc": "Tổng doanh số quý ($)"},
            {"name": "fraud_count", "type": "INT", "key": "", "desc": "Số vụ gian lận quý"},
            {"name": "avg_txn_amount", "type": "FLOAT", "key": "", "desc": "Giá trị trung bình/GD"},
            {"name": "fraud_rate_pct", "type": "FLOAT", "key": "", "desc": "Tỷ lệ gian lận (%)"}
        ]
    },
    {
        "table_name": "agg_daily_card_fraud",
        "category": "Summary",
        "color": "#F43F5E",
        "description": "Giám sát rủi ro thẻ Ngày x Thẻ (Risk Branch)",
        "columns": [
            {"name": "date_sk", "type": "INT", "key": "PK,FK", "ref": "dim_date.date_sk"},
            {"name": "card_sk", "type": "BIGINT", "key": "PK,FK", "ref": "dim_cards.card_sk"},
            {"name": "total_txn", "type": "INT", "key": "", "desc": "Lượt quẹt trong ngày"},
            {"name": "total_amount", "type": "FLOAT", "key": "", "desc": "Tổng tiền quẹt ($)"},
            {"name": "fraud_count", "type": "INT", "key": "", "desc": "Số vụ gian lận"},
            {"name": "avg_amount", "type": "FLOAT", "key": "", "desc": "Giao dịch trung bình"},
            {"name": "max_amount", "type": "FLOAT", "key": "", "desc": "Giao dịch lớn nhất"},
            {"name": "fraud_rate_pct", "type": "FLOAT", "key": "", "desc": "Tỷ lệ gian lận (%)"}
        ]
    }
]

@app.get("/api/schema/metadata")
def get_schema_metadata():
    return SCHEMA_METADATA


class SQLRequest(BaseModel):
    query: str

@app.post("/api/sql/execute")
def execute_sql(req: SQLRequest):
    query = req.query.strip()
    if not query:
        raise HTTPException(status_code=400, detail="Query cannot be empty")
    
    # Chỉ cho phép lệnh SELECT để an toàn
    if not query.lower().startswith("select"):
        raise HTTPException(status_code=400, detail="Chỉ cho phép thực thi câu lệnh SELECT.")

    conn = get_db()
    t0 = time.time()
    try:
        df = pd.read_sql_query(query, conn)
        elapsed_ms = round((time.time() - t0) * 1000, 2)
        # Convert non-serializable objects
        for col in df.columns:
            if df[col].dtype == "object":
                df[col] = df[col].astype(str)
        return {
            "columns": list(df.columns),
            "rows": df.head(100).to_dict(orient="records"),
            "total_rows": len(df),
            "execution_ms": elapsed_ms
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


if __name__ == "__main__":
    print("[INFO] Starting FastAPI server on http://127.0.0.1:8000...")
    uvicorn.run(app, host="127.0.0.1", port=8000)
