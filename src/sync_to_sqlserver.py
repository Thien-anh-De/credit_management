"""
Sync Gold Parquet Data Warehouse to Local Microsoft SQL Server (SSMS)
Allows viewing Star Schema Diagram, Foreign Keys, and running T-SQL queries directly in SSMS.
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

CONN_STR = (
    "Driver={ODBC Driver 18 for SQL Server};"
    "Server=localhost;"
    "Database=CreditCard_DWH;"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)


def get_connection():
    try:
        return pyodbc.connect(CONN_STR)
    except Exception:
        alt_conn_str = CONN_STR.replace("ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server")
        return pyodbc.connect(alt_conn_str)


def clean_dataframe(table_name, df):
    # Chuẩn hóa kiểu dữ liệu cho SQL Server
    if table_name == "dim_users":
        if "apartment" in df.columns:
            df["apartment"] = df["apartment"].apply(lambda x: str(int(x)) if pd.notnull(x) else None)
        if "zipcode" in df.columns:
            df["zipcode"] = df["zipcode"].astype(str)

    if table_name == "dim_cards":
        for col in ["card_number", "expires", "cvv", "card_natural_key"]:
            if col in df.columns:
                df[col] = df[col].astype(str)

    # Chuẩn hóa cột boolean thành int
    for col in df.columns:
        if df[col].dtype == "bool":
            df[col] = df[col].astype(int)

    # Chuẩn hóa các cột ngày tháng
    for col in df.columns:
        if "date" in col:
            df[col] = df[col].apply(lambda x: str(x) if pd.notnull(x) else None)
        elif "timestamp" in col:
            df[col] = pd.to_datetime(df[col]).dt.strftime("%Y-%m-%d %H:%M:%S")

    # Chuyển đổi toàn bộ NaN thành None
    df = df.astype(object).where(pd.notnull(df), None)
    return df


def sync_table(conn, table_name, folder_name):
    folder_path = os.path.join(GOLD_DIR, folder_name)
    if not os.path.exists(folder_path):
        print(f"[-] Bỏ qua '{table_name}': Không tìm thấy thư mục {folder_path}")
        return

    df = pd.read_parquet(folder_path)
    total_rows = len(df)
    print(f"\n[SYNC] Bắt đầu đồng bộ bảng '{table_name}' ({total_rows:,} dòng)...")

    cursor = conn.cursor()
    cursor.fast_executemany = True

    # Tạm thời tắt ràng buộc khóa ngoại để nạp dữ liệu mượt mà
    cursor.execute(f"ALTER TABLE {table_name} NOCHECK CONSTRAINT ALL")
    cursor.execute(f"DELETE FROM {table_name}")
    conn.commit()

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

    # Bật lại ràng buộc khóa ngoại
    cursor.execute(f"ALTER TABLE {table_name} WITH CHECK CHECK CONSTRAINT ALL")
    conn.commit()

    elapsed = time.time() - t0
    print(f"-> Hoàn tất '{table_name}': {total_rows:,} dòng nạp vào SQL Server trong {elapsed:.2f}s!")


def main():
    print("=" * 80)
    print("🚀 ĐỒNG BỘ DWH TỪ GOLD PARQUET SANG MICROSOFT SQL SERVER / SSMS")
    print("=" * 80)

    try:
        conn = get_connection()
        print("[+] Kết nối thành công tới SQL Server (localhost / Database: CreditCard_DWH)")
    except Exception as e:
        print(f"[!] Lỗi kết nối SQL Server: {e}")
        print("Vui lòng đảm bảo dịch vụ MSSQLSERVER đang chạy và database CreditCard_DWH đã được tạo.")
        return

    # Thứ tự nạp: Dimension trước, Fact và Summary sau
    tables = [
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

    total_start = time.time()
    for table_name, folder_name in tables:
        sync_table(conn, table_name, folder_name)

    conn.close()
    print("\n" + "=" * 80)
    print(f"🎉 TẤT CẢ DỮ LIỆU ĐÃ ĐƯỢC ĐỒNG BỘ VÀO SQL SERVER TRONG {time.time()-total_start:.2f}s!")
    print("👉 Bây giờ bạn có thể mở SSMS:")
    print("   1. Connect Server: 'localhost' (Windows Authentication)")
    print("   2. Chọn Database: 'CreditCard_DWH'")
    print("   3. Vào 'Database Diagrams' -> New Database Diagram -> Kéo các bảng ra xem Star Schema")
    print("   4. Mở New Query và viết lệnh SELECT ... JOIN trực tiếp trong SSMS!")
    print("=" * 80)


if __name__ == "__main__":
    main()
