"""
DWH Job 02: Build dim_merchant (Gold Layer)
=============================================
Tách thông tin Merchant từ fact_transactions Silver ra thành bảng chiều riêng.
Gán Surrogate Key (merchant_sk) tăng dần.

Cột đầu ra: merchant_sk | merchant_name | merchant_city | merchant_state | mcc
Đáp ứng: Milestone 3.2 (Star Schema — Merchant Dimension)
Chạy: spark-submit src/dwh_jobs/02_build_dim_merchant.py
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id, trim, upper, when, lit
from pyspark.sql.types import LongType


def main():
    print("=" * 60)
    print("  [DWH JOB 02] BUILD dim_merchant — Gold Layer")
    print("=" * 60)

    base_dir     = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    fact_path    = os.path.join(base_dir, "data", "silver", "fact_transactions")
    output_path  = os.path.join(base_dir, "data", "gold",   "dim_merchant")

    spark = SparkSession.builder \
        .appName("DWH_02_Build_dim_merchant") \
        .master("local[*]") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print(f"-> Đọc fact_transactions Silver từ: {fact_path}")
    fact = spark.read.parquet(fact_path)

    # ── Kiểm tra cột merchant tồn tại ──
    available_cols = [c.lower() for c in fact.columns]
    print(f"-> Các cột có trong fact_transactions: {fact.columns}")

    # Chọn đúng tên cột (tương thích với spark_consumer.py output)
    name_col  = next((c for c in fact.columns if "merchant" in c.lower() and "name"  in c.lower()), None)
    city_col  = next((c for c in fact.columns if "merchant" in c.lower() and "city"  in c.lower()), None)
    state_col = next((c for c in fact.columns if "merchant" in c.lower() and "state" in c.lower()), None)
    mcc_col   = next((c for c in fact.columns if c.lower() == "mcc"), None)

    print(f"-> Mapping cột Merchant: name={name_col}, city={city_col}, state={state_col}, mcc={mcc_col}")

    select_exprs = []
    if name_col:
        select_exprs.append(col(name_col).alias("merchant_name"))
    else:
        select_exprs.append(lit("Unknown").alias("merchant_name"))

    if city_col:
        select_exprs.append(col(city_col).alias("merchant_city"))
    else:
        select_exprs.append(lit("Unknown").alias("merchant_city"))

    if state_col:
        select_exprs.append(col(state_col).alias("merchant_state"))
    else:
        select_exprs.append(lit("Unknown").alias("merchant_state"))

    if mcc_col:
        select_exprs.append(col(mcc_col).cast("string").alias("mcc"))
    else:
        select_exprs.append(lit("0000").alias("mcc"))

    # ── Dedup và gán Surrogate Key ──
    dim_merchant = fact.select(*select_exprs) \
        .dropDuplicates(["merchant_name", "merchant_city", "merchant_state", "mcc"]) \
        .na.fill({"merchant_name": "Unknown", "merchant_city": "Unknown",
                  "merchant_state": "XX", "mcc": "0000"}) \
        .withColumn("merchant_sk", (monotonically_increasing_id() + 1).cast(LongType())) \
        .select("merchant_sk", "merchant_name", "merchant_city", "merchant_state", "mcc")

    row_count = dim_merchant.count()
    print(f"-> Tổng số Merchant duy nhất: {row_count:,}")
    print(f"-> Ghi Parquet vào: {output_path}")

    dim_merchant.write.mode("overwrite").parquet(output_path)
    print("✅ [DWH JOB 02] DONE — dim_merchant đã sẵn sàng tại Gold Layer")
    spark.stop()


if __name__ == "__main__":
    main()
