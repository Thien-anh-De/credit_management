"""
Interactive SQL Query Tool for Credit Management DWH (Kimball Star Schema & Inmon Roll-up)
Allows running arbitrary ANSI SQL queries directly on Gold Parquet layers via PySpark SQL.
"""

import sys
import os
import time

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

from pyspark.sql import SparkSession

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
GOLD_DIR = os.path.join(BASE_DIR, "data", "gold")

PRESET_QUERIES = {
    "1": (
        "Inmon Roll-up: Doanh số & Gian lận cuộn từ Tháng lên Quý (Tháp Merchant)",
        """
        SELECT 
            year,
            quarter,
            COUNT(DISTINCT merchant_sk) AS active_merchants,
            SUM(total_txn) AS quarterly_txns,
            ROUND(SUM(total_volume), 2) AS quarterly_volume,
            SUM(fraud_count) AS quarterly_frauds,
            ROUND(SUM(total_volume) / SUM(total_txn), 2) AS avg_txn_amount,
            ROUND(SUM(fraud_count) * 100.0 / SUM(total_txn), 4) AS fraud_rate_pct
        FROM agg_monthly_merchant_vol
        GROUP BY year, quarter
        ORDER BY year DESC, quarter DESC
        """
    ),
    "2": (
        "Drill-down: Phân tích Rủi ro Gian lận theo Hình thức Quẹt thẻ (Chip vs Swipe)",
        """
        SELECT 
            f.use_chip,
            COUNT(*) AS total_transactions,
            ROUND(SUM(f.amount), 2) AS total_volume,
            SUM(CASE WHEN f.is_fraud THEN 1 ELSE 0 END) AS fraud_count,
            ROUND(SUM(CASE WHEN f.is_fraud THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 4) AS fraud_rate_pct,
            ROUND(AVG(f.amount), 2) AS avg_amount
        FROM fact_transactions f
        GROUP BY f.use_chip
        ORDER BY fraud_rate_pct DESC
        """
    ),
    "3": (
        "Bằng chứng SCD2 Point-in-Time Join: Thẻ 0_0 chia tách giao dịch qua 2 card_sk",
        """
        SELECT 
            f.card_sk,
            c.card_index,
            c.credit_limit,
            c.card_on_dark_web,
            c.effective_date,
            c.expiration_date,
            c.is_current,
            COUNT(*) AS so_giao_dich,
            ROUND(SUM(f.amount), 2) AS tong_tien,
            MIN(f.transaction_timestamp) AS giao_dich_som_nhat,
            MAX(f.transaction_timestamp) AS giao_dich_muon_nhat
        FROM fact_transactions f
        JOIN dim_cards c ON f.card_sk = c.card_sk
        WHERE c.user_id = 0 AND c.card_index = 0
        GROUP BY f.card_sk, c.card_index, c.credit_limit, c.card_on_dark_web, 
                 c.effective_date, c.expiration_date, c.is_current
        ORDER BY f.card_sk
        """
    ),
    "4": (
        "Top 10 Merchant có Doanh số Lớn Nhất & Tỷ lệ Gian lận (Star Schema Join)",
        """
        SELECT 
            m.merchant_name,
            m.merchant_city,
            m.merchant_state,
            COUNT(*) AS total_txns,
            ROUND(SUM(f.amount), 2) AS total_volume,
            SUM(CASE WHEN f.is_fraud THEN 1 ELSE 0 END) AS fraud_count,
            ROUND(SUM(CASE WHEN f.is_fraud THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 4) AS fraud_rate_pct
        FROM fact_transactions f
        JOIN dim_merchant m ON f.merchant_sk = m.merchant_sk
        GROUP BY m.merchant_name, m.merchant_city, m.merchant_state
        ORDER BY total_volume DESC
        LIMIT 10
        """
    ),
    "5": (
        "Slice & Dice: Doanh số & Rủi ro theo Năm và Nhóm Tuổi Khách hàng",
        """
        SELECT 
            d.year,
            CASE 
                WHEN u.current_age < 30 THEN '1. Duoi 30'
                WHEN u.current_age BETWEEN 30 AND 50 THEN '2. Tu 30-50'
                ELSE '3. Tren 50'
            END AS age_group,
            COUNT(*) AS total_txns,
            ROUND(SUM(f.amount), 2) AS total_volume,
            SUM(CASE WHEN f.is_fraud THEN 1 ELSE 0 END) AS fraud_count
        FROM fact_transactions f
        JOIN dim_users u ON f.user_sk = u.user_sk
        JOIN dim_date d ON f.date_sk = d.date_sk
        GROUP BY d.year, age_group
        ORDER BY d.year DESC, age_group
        """
    )
}


def init_spark_and_views():
    spark = (
        SparkSession.builder.appName("DWH_SQL_Studio")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    tables = {
        "fact_transactions": "fact_transactions",
        "dim_cards": "dim_cards",
        "dim_users": "dim_users",
        "dim_merchant": "dim_merchant",
        "dim_date": "dim_date",
        "agg_daily_merchant_vol": "agg_daily_merchant_vol",
        "agg_monthly_merchant_vol": "agg_monthly_merchant_vol",
        "agg_quarterly_merchant_vol": "agg_quarterly_merchant_vol",
        "agg_daily_card_fraud": "agg_daily_card_fraud",
    }

    loaded = []
    for view_name, folder in tables.items():
        path = os.path.join(GOLD_DIR, folder)
        if os.path.exists(path):
            spark.read.parquet(path).createOrReplaceTempView(view_name)
            loaded.append(view_name)

    return spark, loaded


def execute_sql(spark, sql_query):
    print(f"\n[EXEC] Thực thi câu truy vấn SQL:\n{sql_query.strip()}\n")
    t0 = time.time()
    try:
        df = spark.sql(sql_query)
        df.show(50, truncate=False)
        row_count = df.count()
        elapsed = time.time() - t0
        print(f"-> Hoàn tất trong {elapsed:.3f}s. Số bản ghi trả về: {row_count}")
    except Exception as e:
        print(f"[ERROR] Lỗi thực thi SQL: {e}")


def interactive_menu(spark, loaded_tables):
    print("\n" + "=" * 80)
    print("💳 TRUNG TÂM TRUY VẤN DWH SQL (KIMBALL STAR SCHEMA & INMON ROLLUP)")
    print("=" * 80)
    print("Danh sách bảng DWH đã nạp vào SQL Engine:")
    for t in loaded_tables:
        print(f"  • {t}")

    while True:
        print("\n--- MENU TRUY VẤN MẪU (OLAP QUERIES) ---")
        for k, (desc, _) in PRESET_QUERIES.items():
            print(f"[{k}] {desc}")
        print("[custom] Gõ câu truy vấn SQL bất kỳ (hoặc gõ SQL trực tiếp)")
        print("[schema] Xem cấu trúc schema của một bảng")
        print("[exit] Thoát chương trình")

        choice = input("\nNhập lựa chọn của bạn (1-5, custom, schema, exit): ").strip()
        if choice.lower() in ("exit", "quit", "q"):
            print("Tạm biệt!")
            break

        if choice in PRESET_QUERIES:
            desc, sql = PRESET_QUERIES[choice]
            execute_sql(spark, sql)
        elif choice.lower() == "schema":
            tbl = input("Nhập tên bảng muốn xem schema: ").strip()
            if tbl in loaded_tables:
                spark.table(tbl).printSchema()
            else:
                print(f"Không tìm thấy bảng '{tbl}'. Các bảng có sẵn: {', '.join(loaded_tables)}")
        elif choice.lower() == "custom" or choice.lower().startswith("select"):
            if choice.lower().startswith("select"):
                custom_sql = choice
            else:
                print("Nhập câu lệnh SQL của bạn (kết thúc bằng dấu chấm phẩy ; hoặc bấm Enter):")
                lines = []
                while True:
                    line = input()
                    if line.strip().endswith(";"):
                        lines.append(line.rstrip(";"))
                        break
                    elif line.strip() == "":
                        break
                    lines.append(line)
                custom_sql = " ".join(lines)

            if custom_sql.strip():
                execute_sql(spark, custom_sql)
        else:
            print("Lựa chọn không hợp lệ. Vui lòng nhập từ 1 đến 5 hoặc 'custom'.")


def main():
    spark, loaded = init_spark_and_views()
    if len(sys.argv) > 1 and sys.argv[1] == "--sql":
        query = " ".join(sys.argv[2:])
        execute_sql(spark, query)
    elif len(sys.argv) > 1 and sys.argv[1] in PRESET_QUERIES:
        _, query = PRESET_QUERIES[sys.argv[1]]
        execute_sql(spark, query)
    else:
        interactive_menu(spark, loaded)
    spark.stop()


if __name__ == "__main__":
    main()
