"""
DWH Job 04: Build dim_cards với SCD Type 2 (Gold Layer)
=========================================================
Đẩy dim_cards từ Silver sang Gold với:
  - Surrogate Key: card_sk (PK của DWH)
  - SCD Type 2 tracking các cột nhạy cảm: credit_limit, card_on_dark_web
  - 3 cột bắt buộc: effective_date, expiration_date, is_current

Trường hợp điển hình trong nghiệp vụ thẻ ngân hàng:
  - credit_limit: Ngân hàng nâng/hạ hạn mức thẻ theo đánh giá định kỳ
  - card_on_dark_web: FALSE → TRUE khi phát hiện thẻ bị lộ (sự kiện không thể đảo ngược)
    → SCD2 giúp ghi lại CHÍNH XÁC thời điểm phát hiện rò rỉ (forensics audit trail)

Đáp ứng: Milestone 4.2 (SCD Type 2), Bước 2 & 5 (Nonvolatile & Time-variant)
Chạy: spark-submit src/dwh_jobs/04_build_dim_cards_scd2.py
"""
import os
import sys
import argparse
from datetime import date

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, md5, concat_ws, monotonically_increasing_id, when, coalesce, date_format
)
from pyspark.sql.types import LongType, BooleanType

# ─── Cấu hình SCD2 ────────────────────────────────────────────────────────────
SCD_COLS    = ["credit_limit", "card_on_dark_web"]  # Cột theo dõi thay đổi
NATURAL_KEY = "user_id"                             # Khóa tự nhiên (user_id + card_index)
NAT_KEY_2   = "card_index"                         # Compound natural key
BASE_START_DATE = "1970-01-01"                     # Sentinel date (quy ước Unix epoch bao quát toàn bộ quá khứ nếu thiếu ngày mở)
FAR_FUTURE      = "9999-12-31"


def _get_max_sk(df) -> int:
    max_val = df.agg({"card_sk": "max"}).collect()[0][0]
    return int(max_val) if max_val is not None else 0


def main():
    parser = argparse.ArgumentParser(description="Build dim_cards with SCD Type 2")
    parser.add_argument(
        "--effective-date",
        type=str,
        default=os.environ.get("SCD2_EFFECTIVE_DATE", "2015-06-01"),
        help="Effective date for new SCD2 version (default: 2015-06-01 to demonstrate PIT join)"
    )
    args = parser.parse_args()
    eff_date_str = args.effective_date

    print("=" * 60)
    print("  [DWH JOB 04] BUILD dim_cards SCD Type 2 — Gold Layer")
    print(f"  Effective Date for New Versions: {eff_date_str}")
    print("=" * 60)

    base_dir    = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    silver_path = os.path.join(base_dir, "data", "silver", "dim_cards")
    gold_path   = os.path.join(base_dir, "data", "gold",   "dim_cards")

    spark = SparkSession.builder \
        .appName("DWH_04_SCD2_dim_cards") \
        .master("local[*]") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print(f"-> Đọc dim_cards Silver từ: {silver_path}")
    incoming = spark.read.parquet(silver_path)

    # Validate SCD_COLS
    valid_scd = [c for c in SCD_COLS if c in incoming.columns]
    print(f"-> SCD tracking columns: {valid_scd}")
    print(f"-> Schema Silver: {incoming.columns}")

    # Hash để so sánh nhanh
    incoming = incoming.withColumn(
        "row_hash",
        md5(concat_ws("|", *[col(c).cast("string") for c in valid_scd]))
    )

    # ── Compound natural key: user_id + card_index (không có card_id standalone) ──
    incoming = incoming.withColumn(
        "card_natural_key",
        concat_ws("_", col(NATURAL_KEY).cast("string"), col(NAT_KEY_2).cast("string"))
    )

    gold_exists = os.path.exists(gold_path) and bool(os.listdir(gold_path))

    if not gold_exists:
        # ══ LẦN ĐẦU: KHỞI TẠO ══
        print("-> Gold chưa tồn tại. Khởi tạo toàn bộ dim_cards (SCD2 INIT)...")

        # Xác định effective_date cho bản ghi ban đầu:
        # Ưu tiên ngày mở thẻ thực tế acct_open_date; nếu thiếu thì dùng BASE_START_DATE (sentinel date 1970-01-01)
        if "acct_open_date" in incoming.columns:
            init_eff_date = coalesce(date_format(col("acct_open_date"), "yyyy-MM-dd"), lit(BASE_START_DATE))
            print("   -> Sử dụng acct_open_date làm effective_date ban đầu (với fallback sentinel date 1970-01-01)")
        else:
            init_eff_date = lit(BASE_START_DATE)
            print("   -> Sử dụng sentinel date 1970-01-01 làm effective_date ban đầu")

        dim_cards_gold = incoming \
            .withColumn("card_sk",         (monotonically_increasing_id() + 1).cast(LongType())) \
            .withColumn("effective_date",  init_eff_date) \
            .withColumn("expiration_date", lit(FAR_FUTURE)) \
            .withColumn("is_current",      lit(True).cast(BooleanType()))

        row_count = dim_cards_gold.count()
        dim_cards_gold.write.mode("overwrite").parquet(gold_path)
        print(f"✅ INIT DONE — {row_count:,} cards đã được ghi vào Gold với SCD2 cols")

    else:
        # ══ INCREMENTAL UPDATE ══
        print("-> Gold đã tồn tại. Thực hiện SCD Type 2 incremental update...")

        existing = spark.read.parquet(gold_path)
        current  = existing.filter(col("is_current") == True)

        # So sánh qua compound natural key
        changed = current.alias("old").join(
            incoming.alias("new"),
            col("old.card_natural_key") == col("new.card_natural_key"),
            how="inner"
        ).filter(
            col("old.row_hash") != col("new.row_hash")
        ).select(col("old.card_natural_key").alias("changed_key"))

        changed_keys = [r["changed_key"] for r in changed.collect()]
        n_changed    = len(changed_keys)
        print(f"-> Phát hiện {n_changed} card(s) có dữ liệu thay đổi")

        if n_changed > 0:
            # BƯỚC 1: Close record cũ (chỉ cập nhật record is_current của các key có thay đổi)
            closed = existing.withColumn(
                "expiration_date",
                when(
                    col("card_natural_key").isin(changed_keys) & col("is_current"),
                    lit(eff_date_str)
                ).otherwise(col("expiration_date"))
            ).withColumn(
                "is_current",
                when(
                    col("card_natural_key").isin(changed_keys) & col("is_current"),
                    lit(False).cast(BooleanType())
                ).otherwise(col("is_current"))
            )

            # BƯỚC 2: Insert record mới với effective_date theo tham số (bảo đảm Point-in-Time Join)
            max_sk   = _get_max_sk(existing)
            new_rows = incoming.filter(
                col("card_natural_key").isin(changed_keys)
            ).withColumn(
                "card_sk",         (monotonically_increasing_id() + max_sk + 1).cast(LongType())
            ).withColumn(
                "effective_date",  lit(eff_date_str)
            ).withColumn(
                "expiration_date", lit(FAR_FUTURE)
            ).withColumn(
                "is_current",      lit(True).cast(BooleanType())
            )

            # BƯỚC 3: Merge và ghi Gold an toàn qua staging directory
            # Toàn bộ lịch sử cũ (closed records) + phiên bản mới được giữ nguyên (Nonvolatile)
            temp_gold = gold_path + "_stage"
            final_df = closed.unionByName(new_rows, allowMissingColumns=True)
            final_df.write.mode("overwrite").parquet(temp_gold)
            import shutil
            shutil.rmtree(gold_path, ignore_errors=True)
            shutil.move(temp_gold, gold_path)
            print(f"✅ SCD2 UPDATE DONE — {n_changed} cards đã được versioned với effective_date = {eff_date_str}")
        else:
            print("-> Không có thay đổi. Toàn bộ phiên bản SCD2 trong Gold dim_cards được bảo toàn (Nonvolatile).")

    # Thống kê
    result = spark.read.parquet(gold_path)
    total  = result.count()
    active = result.filter(col("is_current") == True).count()
    dark_web_alerts = result.filter(
        (col("is_current") == True) & (col("card_on_dark_web") == True)
    ).count()
    print(f"\n📊 dim_cards Gold stats:")
    print(f"   Total rows (all versions): {total:,}")
    print(f"   Active cards (is_current=True): {active:,}")
    print(f"   🚨 Dark Web Alerts (active): {dark_web_alerts:,}")
    print("✅ [DWH JOB 04] DONE")
    spark.stop()


if __name__ == "__main__":
    main()
