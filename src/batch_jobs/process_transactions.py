"""
Batch Job: Process Transactions (Bronze to Silver)
===================================================
Chuyển đổi dữ liệu giao dịch từ Bronze CSV sang Silver Parquet.
Phục vụ chạy batch pipeline và phát triển offline khi không bật Kafka cluster.
Logic làm sạch đồng nhất 100% với spark_consumer.py.
"""
import os
import sys
import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, concat_ws, lpad, when, upper, trim, coalesce, lit, regexp_replace, date_format
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, DoubleType
)


def main():
    parser = argparse.ArgumentParser(description="Process Bronze Transactions to Silver Parquet")
    parser.add_argument("--source", type=str, default="", help="Path to bronze CSV (optional)")
    parser.add_argument("--limit", type=int, default=0, help="Row limit (0 = all)")
    args = parser.parse_args()

    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

    # Xác định file nguồn
    default_user0 = os.path.join(base_dir, "data", "bronze", "User0_credit_card_transactions.csv")
    default_big   = os.path.join(base_dir, "data", "bronze", "credit_card_transactions-ibm_v2.csv")

    if args.source:
        src_lower = args.source.strip().lower()
        if src_lower in ["sample", "user0", "small"]:
            csv_path = default_user0
        elif src_lower in ["full", "big", "ibm_v2", "all"]:
            csv_path = default_big
        else:
            csv_path = args.source
    else:
        # Ưu tiên User0 nếu muốn chạy nhanh test, hoặc big CSV nếu muốn đầy đủ
        csv_path = default_user0 if os.path.exists(default_user0) else default_big

    output_path = os.path.join(base_dir, "data", "silver", "fact_transactions")

    print(f"-> Source Bronze CSV : {csv_path}")
    print(f"-> Target Silver Path: {output_path}")

    spark = SparkSession.builder \
        .appName("ProcessTransactionsBatch") \
        .master("local[*]") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Đọc CSV Bronze
    df_raw = spark.read.csv(csv_path, header=True, inferSchema=True)
    if args.limit > 0:
        df_raw = df_raw.limit(args.limit)

    # Đổi tên cột và làm sạch
    col_map = {c.lower().replace("?", "").replace(" ", "_"): c for c in df_raw.columns}

    user_c   = col_map.get("user", "User")
    card_c   = col_map.get("card", "Card")
    year_c   = col_map.get("year", "Year")
    month_c  = col_map.get("month", "Month")
    day_c    = col_map.get("day", "Day")
    time_c   = col_map.get("time", "Time")
    amount_c = col_map.get("amount", "Amount")
    chip_c   = col_map.get("use_chip", "Use Chip")
    m_name_c = col_map.get("merchant_name", "Merchant Name")
    m_city_c = col_map.get("merchant_city", "Merchant City")
    m_stat_c = col_map.get("merchant_state", "Merchant State")
    zip_c    = col_map.get("zip", "Zip")
    mcc_c    = col_map.get("mcc", "MCC")
    err_c    = col_map.get("errors", "Errors?")
    fraud_c  = col_map.get("is_fraud", "Is Fraud?")

    # Rút trích giờ phút chính xác (phòng trường hợp inferSchema ép Time thành timestamp)
    time_str = when(col(time_c).cast("string").contains("-"), date_format(col(time_c), "HH:mm")).otherwise(col(time_c).cast("string"))
    dt_str = concat_ws(" ",
        concat_ws("-", col(year_c), lpad(col(month_c).cast("string"), 2, "0"), lpad(col(day_c).cast("string"), 2, "0")),
        time_str
    )

    df_clean = df_raw \
        .withColumn("user_id", col(user_c).cast(LongType())) \
        .withColumn("card_index", col(card_c).cast(IntegerType())) \
        .withColumn("transaction_timestamp", to_timestamp(dt_str, "yyyy-MM-dd HH:mm")) \
        .withColumn("amount", regexp_replace(col(amount_c).cast("string"), r"[\$,]", "").cast(DoubleType())) \
        .withColumn("use_chip", col(chip_c).cast(StringType())) \
        .withColumn("merchant_name", col(m_name_c).cast(StringType())) \
        .withColumn("merchant_city", when(col(m_city_c).isNull() | (col(m_city_c) == ""), "Unknown").otherwise(col(m_city_c))) \
        .withColumn("merchant_state", when(col(m_stat_c).isNull() | (col(m_stat_c) == ""), "Unknown").otherwise(col(m_stat_c))) \
        .withColumn("zip", col(zip_c).cast(StringType())) \
        .withColumn("mcc", col(mcc_c).cast(StringType())) \
        .withColumn("errors", when(col(err_c).isNull() | (col(err_c) == ""), "None").otherwise(col(err_c))) \
        .withColumn("is_fraud", when(upper(trim(col(fraud_c).cast("string"))) == "YES", True).otherwise(False)) \
        .withColumn("current_timestamp", lit(datetime.now().isoformat())) \
        .withColumn("ingestion_timestamp", lit(datetime.now().isoformat()))

    final_cols = [
        "user_id", "card_index", "transaction_timestamp", "amount",
        "use_chip", "merchant_name", "merchant_city", "merchant_state",
        "zip", "mcc", "errors", "is_fraud", "current_timestamp", "ingestion_timestamp"
    ]
    df_final = df_clean.select(*final_cols)

    row_count = df_final.count()
    print(f"-> Tổng số dòng giao dịch chuyển đổi: {row_count:,}")
    print(f"-> Ghi Parquet vào: {output_path}")

    df_final.write.mode("overwrite").parquet(output_path)
    print("✅ [BATCH JOB] Chuyển đổi Bronze fact sang Silver Parquet THÀNH CÔNG")
    spark.stop()


if __name__ == "__main__":
    main()
