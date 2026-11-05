"""
DWH Job 03: Build dim_users với SCD Type 2 (Gold Layer)
=========================================================
Đẩy dim_users từ Silver sang Gold với:
  - Surrogate Key: user_sk (PK của DWH, thay thế user_id tự nhiên)
  - SCD Type 2 tracking các cột nhạy cảm: address, yearly_income, fico_score
  - 3 cột bắt buộc: effective_date, expiration_date, is_current

Logic SCD2:
  - Lần đầu tiên: INIT toàn bộ bảng (is_current=True, expiration_date=9999-12-31)
  - Lần sau (incremental): So sánh MD5 hash của SCD_COLS
      → Nếu hash thay đổi: CLOSE record cũ + INSERT record mới
      → Nếu không thay đổi: Giữ nguyên

Đáp ứng: Milestone 4.2 (SCD Type 2), Bước 2 & 5 (Time-variant & Nonvolatile)
Chạy: spark-submit src/dwh_jobs/03_build_dim_users_scd2.py
"""
import os
import sys
import shutil
import argparse
from datetime import date

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, md5, concat_ws, monotonically_increasing_id, when
)
from pyspark.sql.types import LongType, BooleanType

# ─── Cấu hình SCD2 ────────────────────────────────────────────────────────────
SCD_COLS   = ["address", "yearly_income", "fico_score"]  # Cột theo dõi thay đổi
NATURAL_KEY = "user_id"                                   # Khóa tự nhiên từ Silver
BASE_START_DATE = "1970-01-01"                            # Sentinel date: mốc Unix epoch quy ước bao quát toàn bộ quá khứ trước khi phát sinh quan hệ tín dụng
FAR_FUTURE      = "9999-12-31"                            # Ngày không bao giờ hết hạn


def _get_max_sk(df) -> int:
    """Lấy giá trị user_sk lớn nhất hiện tại trong Gold."""
    max_val = df.agg({"user_sk": "max"}).collect()[0][0]
    return int(max_val) if max_val is not None else 0


def main():
    parser = argparse.ArgumentParser(description="Build dim_users with SCD Type 2")
    parser.add_argument(
        "--effective-date",
        type=str,
        default=os.environ.get("SCD2_EFFECTIVE_DATE", "2015-06-01"),
        help="Effective date for new SCD2 version (default: 2015-06-01)"
    )
    args = parser.parse_args()
    eff_date_str = args.effective_date

    print("=" * 60)
    print("  [DWH JOB 03] BUILD dim_users SCD Type 2 — Gold Layer")
    print(f"  Effective Date for New Versions: {eff_date_str}")
    print("=" * 60)

    base_dir    = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    silver_path = os.path.join(base_dir, "data", "silver", "dim_users")
    gold_path   = os.path.join(base_dir, "data", "gold",   "dim_users")

    spark = SparkSession.builder \
        .appName("DWH_03_SCD2_dim_users") \
        .master("local[*]") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # ── Đọc dữ liệu Silver (incoming) ──
    print(f"-> Đọc dim_users Silver từ: {silver_path}")
    incoming = spark.read.parquet(silver_path)

    # Đảm bảo SCD_COLS tồn tại trong Silver
    valid_scd = [c for c in SCD_COLS if c in incoming.columns]
    if not valid_scd:
        print("⚠️  Không tìm thấy SCD_COLS trong Silver. Kiểm tra lại schema.")
        spark.stop()
        return

    print(f"-> SCD tracking columns: {valid_scd}")

    # Tính hash row để so sánh nhanh
    incoming = incoming.withColumn(
        "row_hash",
        md5(concat_ws("|", *[col(c) for c in valid_scd]))
    )

    # ── Kiểm tra Gold đã tồn tại chưa ──
    gold_exists = os.path.exists(gold_path) and bool(os.listdir(gold_path))

    if not gold_exists:
        # ══ LẦN ĐẦU: KHỞI TẠO TOÀN BỘ GOLD ══
        print("-> Gold chưa tồn tại. Khởi tạo toàn bộ dim_users (SCD2 INIT)...")
        print("   -> Sử dụng sentinel date 1970-01-01 làm mốc effective_date ban đầu")

        dim_users_gold = incoming \
            .withColumn("user_sk",         (monotonically_increasing_id() + 1).cast(LongType())) \
            .withColumn("effective_date",  lit(BASE_START_DATE)) \
            .withColumn("expiration_date", lit(FAR_FUTURE)) \
            .withColumn("is_current",      lit(True).cast(BooleanType()))

        row_count = dim_users_gold.count()
        dim_users_gold.write.mode("overwrite").parquet(gold_path)
        print(f"✅ INIT DONE — {row_count:,} users đã được ghi vào Gold với SCD2 cols")

    else:
        # ══ INCREMENTAL UPDATE: SCD TYPE 2 ══
        print("-> Gold đã tồn tại. Thực hiện SCD Type 2 incremental update...")

        existing = spark.read.parquet(gold_path)
        current  = existing.filter(col("is_current") == True)

        # JOIN để tìm các record có hash thay đổi
        changed = current.alias("old").join(
            incoming.alias("new"),
            col(f"old.{NATURAL_KEY}") == col(f"new.{NATURAL_KEY}"),
            how="inner"
        ).filter(
            col("old.row_hash") != col("new.row_hash")
        ).select(col(f"old.{NATURAL_KEY}").alias("changed_id"))

        changed_ids = [r["changed_id"] for r in changed.collect()]
        n_changed   = len(changed_ids)
        print(f"-> Phát hiện {n_changed} user(s) có dữ liệu thay đổi (SCD2 update)")

        if n_changed == 0:
            print("-> Không có thay đổi. Toàn bộ phiên bản SCD2 trong Gold dim_users được bảo toàn (Nonvolatile).")
        else:
            # BƯỚC 1: Close bản ghi cũ
            closed = existing.withColumn(
                "expiration_date",
                when(
                    col(NATURAL_KEY).isin(changed_ids) & col("is_current"),
                    lit(eff_date_str)
                ).otherwise(col("expiration_date"))
            ).withColumn(
                "is_current",
                when(
                    col(NATURAL_KEY).isin(changed_ids) & col("is_current"),
                    lit(False).cast(BooleanType())
                ).otherwise(col("is_current"))
            )

            # BƯỚC 2: Tạo bản ghi mới từ incoming data
            max_sk = _get_max_sk(existing)
            new_rows = incoming.filter(
                col(NATURAL_KEY).isin(changed_ids)
            ).withColumn(
                "user_sk",        (monotonically_increasing_id() + max_sk + 1).cast(LongType())
            ).withColumn(
                "effective_date",  lit(eff_date_str)
            ).withColumn(
                "expiration_date", lit(FAR_FUTURE)
            ).withColumn(
                "is_current",      lit(True).cast(BooleanType())
            )

            # BƯỚC 3: Gộp và ghi đè Gold an toàn qua staging directory
            temp_gold = gold_path + "_stage"
            final = closed.unionByName(new_rows, allowMissingColumns=True)
            final.write.mode("overwrite").parquet(temp_gold)
            shutil.rmtree(gold_path, ignore_errors=True)
            shutil.move(temp_gold, gold_path)
            print(f"✅ SCD2 UPDATE DONE — {n_changed} records đã được versioned với effective_date = {eff_date_str}")

    # Thống kê final
    result = spark.read.parquet(gold_path)
    total  = result.count()
    active = result.filter(col("is_current") == True).count()
    print(f"\n📊 dim_users Gold stats:")
    print(f"   Total rows (all versions): {total:,}")
    print(f"   Active records (is_current=True): {active:,}")
    print("✅ [DWH JOB 03] DONE")
    spark.stop()


if __name__ == "__main__":
    main()
