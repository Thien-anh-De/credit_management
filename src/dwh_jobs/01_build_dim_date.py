"""
DWH Job 01: Build dim_date (Gold Layer)
========================================
Sinh bảng chiều thời gian chuẩn Kimball từ 2010-01-01 đến 2030-12-31.
Không phụ thuộc dữ liệu giao dịch — chạy độc lập một lần duy nhất.

Đáp ứng: Milestone 3.2 (Star Schema — Time Dimension)
Chạy: spark-submit src/dwh_jobs/01_build_dim_date.py
"""
import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, date_format, dayofweek, dayofmonth,
    weekofyear, month, quarter, year, lit
)
from pyspark.sql.types import IntegerType, BooleanType

# ─── Các ngày lễ tại Việt Nam (ví dụ mẫu — có thể mở rộng) ────────────────
VN_HOLIDAYS = {
    "01-01",  # Tết Dương Lịch
    "04-30",  # Ngày Giải phóng Miền Nam
    "05-01",  # Ngày Quốc tế Lao động
    "09-02",  # Quốc khánh
}


def main():
    print("=" * 60)
    print("  [DWH JOB 01] BUILD dim_date — Gold Layer")
    print("=" * 60)

    base_dir    = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    output_path = os.path.join(base_dir, "data", "gold", "dim_date")

    spark = SparkSession.builder \
        .appName("DWH_01_Build_dim_date") \
        .master("local[*]") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # ── Sinh dãy ngày trực tiếp bằng Spark SQL (1995–2035, bao phủ toàn bộ lịch sử) ──
    df = spark.sql("""
        SELECT explode(sequence(to_date('1995-01-01'), to_date('2035-12-31'), interval 1 day)) as full_date
    """)

    df = df \
        .withColumn("date_sk",       date_format("full_date", "yyyyMMdd").cast(IntegerType())) \
        .withColumn("full_date_str", date_format("full_date", "yyyy-MM-dd")) \
        .withColumn("day_of_week",   dayofweek("full_date")) \
        .withColumn("day_name",      date_format("full_date", "EEEE")) \
        .withColumn("day_of_month",  dayofmonth("full_date")) \
        .withColumn("week_of_year",  weekofyear("full_date")) \
        .withColumn("month_number",  month("full_date")) \
        .withColumn("month_name",    date_format("full_date", "MMMM")) \
        .withColumn("quarter",       quarter("full_date")) \
        .withColumn("year",          year("full_date")) \
        .withColumn("is_weekend",    dayofweek("full_date").isin([1, 7]).cast(BooleanType())) \
        .withColumn("mmdd",          date_format("full_date", "MM-dd")) \
        .withColumn("is_holiday_vn", col("mmdd").isin(list(VN_HOLIDAYS)).cast(BooleanType())) \
        .drop("mmdd", "full_date")

    # Sắp xếp cột theo thứ tự chuẩn Kimball
    final_cols = [
        "date_sk", "full_date_str", "day_of_week", "day_name",
        "day_of_month", "week_of_year", "month_number", "month_name",
        "quarter", "year", "is_weekend", "is_holiday_vn"
    ]
    df = df.select(*final_cols)

    row_count = df.count()
    print(f"-> Tổng số ngày sinh ra: {row_count:,} rows (2010–2030)")
    print(f"-> Ghi Parquet vào: {output_path}")

    df.write.mode("overwrite").parquet(output_path)

    print("✅ [DWH JOB 01] DONE — dim_date đã sẵn sàng tại Gold Layer")
    spark.stop()


if __name__ == "__main__":
    main()
