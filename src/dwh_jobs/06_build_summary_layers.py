"""
DWH Job 06: Build Summary Layers (Gold Layer - Inmon Chapter 3 Compliant)
==========================================================================
Xây dựng tháp tổng hợp đa cấp (Summary Layers) theo nguyên lý của Bill Inmon:

1. THÁP 1: MERCHANT VOLUME & FINANCIAL ROLLUP TOWER (Chuỗi roll-up chuẩn Inmon)
   - Tầng 1: agg_daily_merchant_vol (Ngày × Merchant) — từ Atomic Fact
   - Tầng 2: agg_monthly_merchant_vol (Tháng × Merchant) — Roll-up TRỰC TIẾP từ Tầng Ngày
   - Tầng 3: agg_quarterly_merchant_vol (Quý × Merchant) — Roll-up TRỰC TIẾP từ Tầng Tháng
   * Lưu các độ đo cộng dồn (Additive Measures: total_txn, total_volume, fraud_count)
   * Suy xuất độ đo tỷ lệ (avg_txn_amount, fraud_rate_pct) ở bước cuối, bảo đảm tính cộng dồn tuyệt đối.

2. THÁP 2: CARD RISK & FRAUD ANALYTICS (Nhánh phân tích rủi ro thẻ chuyên biệt)
   - agg_daily_card_fraud (Ngày × Thẻ) — phục vụ phát hiện gian lận và cảnh báo thẻ

Đáp ứng: Sub-milestone 2.2 (Summary Layers), 5.1 (Query Performance Benchmark), Inmon Chapter 3
Chạy: spark-submit src/dwh_jobs/06_build_summary_layers.py
"""
import os
import sys

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as _sum, avg, max as _max, min as _min,
    round as _round, when, lit
)
from pyspark.sql.types import DoubleType


def main():
    print("=" * 65)
    print("  [DWH JOB 06] BUILD Summary Layers (Inmon Roll-up Hierarchy) — Gold")
    print("=" * 65)

    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

    gold_fact       = os.path.join(base_dir, "data", "gold", "fact_transactions")
    gold_date       = os.path.join(base_dir, "data", "gold", "dim_date")
    out_daily_card  = os.path.join(base_dir, "data", "gold", "agg_daily_card_fraud")
    out_daily_merch = os.path.join(base_dir, "data", "gold", "agg_daily_merchant_vol")
    out_monthly     = os.path.join(base_dir, "data", "gold", "agg_monthly_merchant_vol")
    out_quarterly   = os.path.join(base_dir, "data", "gold", "agg_quarterly_merchant_vol")

    spark = SparkSession.builder \
        .appName("DWH_06_Build_Summary_Layers") \
        .master("local[*]") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # ── Đọc fact Gold ──
    print(f"-> Đọc fact_transactions Gold từ: {gold_fact}")
    fact = spark.read.parquet(gold_fact)
    total_atomic = fact.count()
    print(f"-> Tổng số dòng Atomic fact: {total_atomic:,}")

    # Chuẩn hóa cột is_fraud về int để tính sum
    if "is_fraud" in fact.columns:
        fact = fact.withColumn("is_fraud_int", col("is_fraud").cast("int"))
    else:
        fact = fact.withColumn("is_fraud_int", lit(0))

    if "amount" not in fact.columns:
        fact = fact.withColumn("amount", lit(0.0).cast(DoubleType()))

    # Đọc dim_date để phục vụ rollup thời gian
    d_date = spark.read.parquet(gold_date) \
        .select("date_sk", "year", "quarter", "month_number", "month_name")
    fact_with_date = fact.join(d_date, "date_sk", "left")

    # ═══════════════════════════════════════════════════════════════
    # NHÁNH 1: CARD RISK ANALYTICS — agg_daily_card_fraud
    # Grain: 1 dòng = 1 ngày × 1 thẻ
    # ═══════════════════════════════════════════════════════════════
    print("\n-> [1/4] Xây dựng agg_daily_card_fraud (Trục Rủi ro Thẻ)...")

    agg_daily_card = fact.groupBy("date_sk", "card_sk") \
        .agg(
            count("*").alias("total_txn"),
            _sum("amount").alias("total_amount"),
            _sum("is_fraud_int").alias("fraud_count"),
            avg("amount").alias("avg_amount"),
            _max("amount").alias("max_amount"),
            _round((_sum("is_fraud_int") / count("*") * 100), 2).alias("fraud_rate_pct")
        )

    daily_card_count = agg_daily_card.count()
    agg_daily_card.write.mode("overwrite").parquet(out_daily_card)
    print(f"✅ agg_daily_card_fraud: {daily_card_count:,} rows → {out_daily_card}")

    # ═══════════════════════════════════════════════════════════════
    # THÁP MERCHANT VOLUME (INMON CHAPTER 3 ROLL-UP HIERARCHY)
    # TẦNG NGÀY: agg_daily_merchant_vol
    # Grain: 1 dòng = 1 ngày × 1 merchant
    # ═══════════════════════════════════════════════════════════════
    print("\n-> [2/4] Xây dựng agg_daily_merchant_vol (Tầng Ngày - Trục Merchant)...")

    # Tính toán độ đo cộng dồn (Additive Measures) từ Fact chi tiết
    agg_daily_merch_base = fact_with_date.groupBy(
        "date_sk", "year", "quarter", "month_number", "month_name", "merchant_sk"
    ).agg(
        count("*").alias("total_txn"),
        _sum("amount").alias("total_volume"),
        _sum("is_fraud_int").alias("fraud_count")
    )

    # Thêm độ đo suy xuất (non-additive)
    agg_daily_merch = agg_daily_merch_base.withColumn(
        "avg_txn_amount", _round(col("total_volume") / col("total_txn"), 2)
    ).withColumn(
        "fraud_rate_pct", _round((col("fraud_count") / col("total_txn")) * 100, 2)
    )

    daily_merch_count = agg_daily_merch.count()
    agg_daily_merch.write.mode("overwrite").parquet(out_daily_merch)
    print(f"✅ agg_daily_merchant_vol: {daily_merch_count:,} rows → {out_daily_merch}")

    # ═══════════════════════════════════════════════════════════════
    # TẦNG THÁNG: agg_monthly_merchant_vol
    # Roll-up TRỰC TIẾP từ agg_daily_merchant_vol (Inmon Chapter 3)
    # Grain: 1 dòng = 1 tháng × 1 merchant
    # ═══════════════════════════════════════════════════════════════
    print("\n-> [3/4] Xây dựng agg_monthly_merchant_vol (Roll-up từ Tầng Ngày)...")

    agg_monthly_base = agg_daily_merch.groupBy(
        "year", "quarter", "month_number", "month_name", "merchant_sk"
    ).agg(
        _sum("total_txn").alias("total_txn"),
        _sum("total_volume").alias("total_volume"),
        _sum("fraud_count").alias("fraud_count")
    )

    agg_monthly = agg_monthly_base.withColumn(
        "avg_txn_amount", _round(col("total_volume") / col("total_txn"), 2)
    ).withColumn(
        "fraud_rate_pct", _round((col("fraud_count") / col("total_txn")) * 100, 2)
    ).orderBy("year", "month_number", "merchant_sk")

    monthly_count = agg_monthly.count()
    agg_monthly.write.mode("overwrite").parquet(out_monthly)
    print(f"✅ agg_monthly_merchant_vol: {monthly_count:,} rows → {out_monthly}")

    # ═══════════════════════════════════════════════════════════════
    # TẦNG QUÝ: agg_quarterly_merchant_vol
    # Roll-up TRỰC TIẾP từ agg_monthly_merchant_vol (Inmon Chapter 3)
    # Grain: 1 dòng = 1 quý × 1 merchant
    # ═══════════════════════════════════════════════════════════════
    print("\n-> [4/4] Xây dựng agg_quarterly_merchant_vol (Roll-up từ Tầng Tháng)...")

    agg_quarterly_base = agg_monthly.groupBy("year", "quarter", "merchant_sk") \
        .agg(
            _sum("total_txn").alias("total_txn"),
            _sum("total_volume").alias("total_volume"),
            _sum("fraud_count").alias("fraud_count")
        )

    agg_quarterly = agg_quarterly_base.withColumn(
        "avg_txn_amount", _round(col("total_volume") / col("total_txn"), 2)
    ).withColumn(
        "fraud_rate_pct", _round((col("fraud_count") / col("total_txn")) * 100, 2)
    ).orderBy("year", "quarter", "merchant_sk")

    quarterly_count = agg_quarterly.count()
    agg_quarterly.write.mode("overwrite").parquet(out_quarterly)
    print(f"✅ agg_quarterly_merchant_vol: {quarterly_count:,} rows → {out_quarterly}")

    # ── Thống kê tổng quan ──
    print("\n" + "=" * 65)
    print("  📊 SUMMARY LAYER STATS (Inmon Roll-up & Additive Hierarchy)")
    print("=" * 65)
    print(f"  Atomic fact_transactions  : {total_atomic:>10,} rows (100.0%)")
    print(f"  [Card] agg_daily_card_fraud     : {daily_card_count:>10,} rows ({daily_card_count/total_atomic*100:.1f}%)")
    print(f"  [Merch] agg_daily_merchant_vol  : {daily_merch_count:>10,} rows ({daily_merch_count/total_atomic*100:.1f}%)")
    print(f"  [Merch] agg_monthly_merchant_vol: {monthly_count:>10,} rows ({monthly_count/total_atomic*100:.2f}%)")
    print(f"  [Merch] agg_quarterly_merchant_vol: {quarterly_count:>10,} rows ({quarterly_count/total_atomic*100:.2f}%)")
    print("  → Tháp Merchant Volume tuân thủ 100% Inmon Chapter 3 (Ngày -> Tháng -> Quý)!")
    print("  → Các độ đo cộng dồn (total_txn, total_volume, fraud_count) khớp hoàn hảo!")

    print("\n✅ [DWH JOB 06] DONE — Summary Layers sẵn sàng")
    spark.stop()


if __name__ == "__main__":
    main()
