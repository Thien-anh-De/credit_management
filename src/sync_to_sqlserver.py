"""
Nạp dữ liệu từ các tệp Parquet tầng Gold vào Microsoft SQL Server (SSMS).
Tự động quét và đồng bộ vào tất cả các Instance đang chạy:
  - localhost (MSSQLSERVER)
  - .\\SQLEXPRESS
  - .\\SQLEXPRESS01
"""

import os
import sys
import time
import pandas as pd
import numpy as np
import pyodbc

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
GOLD_DIR = os.path.join(BASE_DIR, "data", "gold")

INSTANCES = [r".\SQLEXPRESS", "localhost", r".\SQLEXPRESS01"]


def get_connection(server_name):
    conn_str = (
        f"Driver={{ODBC Driver 18 for SQL Server}};"
        f"Server={server_name};"
        f"Database=CreditCard_DWH;"
        f"Trusted_Connection=yes;"
        f"TrustServerCertificate=yes;"
    )
    try:
        return pyodbc.connect(conn_str, timeout=3)
    except Exception:
        alt_str = conn_str.replace("ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server")
        return pyodbc.connect(alt_str, timeout=3)


def clean_dataframe(table_name, df):
    if table_name == "dim_users":
        if "apartment" in df.columns:
            df["apartment"] = df["apartment"].apply(lambda x: str(int(x)) if pd.notnull(x) else None)
        if "zipcode" in df.columns:
            df["zipcode"] = df["zipcode"].astype(str)

    if table_name == "dim_cards":
        for col in ["card_number", "expires", "cvv", "card_natural_key"]:
            if col in df.columns:
                df[col] = df[col].astype(str)

    for col in df.columns:
        if df[col].dtype == "bool":
            df[col] = df[col].astype(int)

    for col in df.columns:
        if "date" in col:
            df[col] = df[col].apply(lambda x: str(x) if pd.notnull(x) else None)
        elif "timestamp" in col:
            df[col] = pd.to_datetime(df[col]).dt.strftime("%Y-%m-%d %H:%M:%S")

    df = df.astype(object).where(pd.notnull(df), None)
    return df


def insert_table(conn, table_name, folder_name):
    folder_path = os.path.join(GOLD_DIR, folder_name)
    if not os.path.exists(folder_path):
        return

    df = pd.read_parquet(folder_path)
    total_rows = len(df)

    cursor = conn.cursor()
    cursor.fast_executemany = True

    df = clean_dataframe(table_name, df)

    cols = list(df.columns)
    col_str = ", ".join([f"[{c}]" for c in cols])
    placeholders = ", ".join(["?"] * len(cols))
    insert_sql = f"INSERT INTO {table_name} ({col_str}) VALUES ({placeholders})"

    batch_size = 5000
    t0 = time.time()
    for i in range(0, total_rows, batch_size):
        chunk = df.iloc[i : i + batch_size]
        data = [tuple(x) for x in chunk.values]
        cursor.executemany(insert_sql, data)
        conn.commit()

    elapsed = time.time() - t0
    print(f"  [OK] {table_name}: Đã nạp {total_rows:,} dòng ({elapsed:.2f}s)")


def sync_instance(server_name):
    try:
        conn = get_connection(server_name)
        print(f"\n[INFO] Đang đồng bộ tới SQL Server Instance: [{server_name}]...")
    except Exception:
        # Không có database hoặc instance không chạy
        return False

    cursor = conn.cursor()

    tables_delete_order = [
        "fact_transactions",
        "agg_daily_merchant_vol",
        "agg_monthly_merchant_vol",
        "agg_quarterly_merchant_vol",
        "agg_daily_card_fraud",
        "dim_cards",
        "dim_users",
        "dim_merchant",
        "dim_date"
    ]

    for tbl in tables_delete_order:
        try:
            cursor.execute(f"ALTER TABLE {tbl} NOCHECK CONSTRAINT ALL")
            cursor.execute(f"DELETE FROM {tbl}")
            conn.commit()
        except Exception:
            pass

    tables_insert_order = [
        ("dim_date", "dim_date"),
        ("dim_merchant", "dim_merchant"),
        ("dim_users", "dim_users"),
        ("dim_cards", "dim_cards"),
        ("fact_transactions", "fact_transactions"),
        ("agg_daily_merchant_vol", "agg_daily_merchant_vol"),
        ("agg_monthly_merchant_vol", "agg_monthly_merchant_vol"),
        ("agg_quarterly_merchant_vol", "agg_quarterly_merchant_vol"),
        ("agg_daily_card_fraud", "agg_daily_card_fraud"),
    ]

    t_start = time.time()
    for table_name, folder_name in tables_insert_order:
        insert_table(conn, table_name, folder_name)

    for tbl in tables_delete_order:
        try:
            cursor.execute(f"ALTER TABLE {tbl} WITH CHECK CHECK CONSTRAINT ALL")
            conn.commit()
        except Exception:
            pass

    conn.close()
    print(f"[INFO] Hoàn tất nạp dữ liệu cho [{server_name}] trong {time.time() - t_start:.2f}s.")
    return True


def main():
    print("[INFO] Bắt đầu đồng bộ dữ liệu từ Gold Parquet vào SQL Server...")
    synced_count = 0
    for inst in INSTANCES:
        if sync_instance(inst):
            synced_count += 1

    if synced_count == 0:
        print("[ERROR] Không tìm thấy database CreditCard_DWH trên bất kỳ SQL Server instance nào.")
        print("[TIP] Hãy mở SSMS và chạy file docs/dwh_schema_ssms.sql trước.")
    else:
        print(f"\n[SUCCESS] Đã đồng bộ thành công vào {synced_count} SQL Server instance!")


if __name__ == "__main__":
    main()
