"""
DWH Job 05: Build fact_transactions (Gold Layer)
==================================================
Xây dựng bảng fact_transactions Gold chuẩn Kimball:
  - Thay thế FK thô (user_id, card_id, merchant_name...) bằng Surrogate Keys
  - Giữ lại Degenerate Dimension: transaction_id (sinh MD5 deterministic để bảo đảm Idempotent)
  - Thêm date_sk (integer YYYYMMDD) làm FK → dim_date
  - SCD TYPE 2 POINT-IN-TIME JOIN:
      Join theo điều kiện thời gian:
      effective_date <= txn_date < expiration_date
      → Đảm bảo giao dịch quá khứ gắn đúng version quá khứ, không gán nhầm version hiện tại.

Mức hạt (Grain): 1 dòng = 1 giao dịch quẹt thẻ (Atomic Grain — Milestone 2.1)

Schema đầu ra:
  transaction_id | date_sk | user_sk | card_sk | merchant_sk |
  amount | is_fraud | use_chip | transaction_timestamp

Đáp ứng: Milestone 2.1 (Atomic Grain), 3.2 (Star Schema), 4.2 (SCD2 PIT Join), Nonvolatile (Immutable Events), Idempotent (Deterministic Natural Hashing)
Chạy: spark-submit src/dwh_jobs/05_build_fact_transactions.py
"""
import os
import sys
import argparse

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, date_format, lit, coalesce, md5, concat_ws
)
from pyspark.sql.types import IntegerType, LongType


def main():
    parser = argparse.ArgumentParser(description="DWH 05: Build fact_transactions with SCD2 PIT Join")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of rows to process (0 = all)")
    args = parser.parse_args()

    print("=" * 60)
    print("  [DWH JOB 05] BUILD fact_transactions (SCD2 Point-in-Time Join) — Gold Layer")
    print("=" * 60)

    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

    # Paths
    silver_fact   = os.path.join(base_dir, "data", "silver", "fact_transactions")
    gold_users    = os.path.join(base_dir, "data", "gold",   "dim_users")
    gold_cards    = os.path.join(base_dir, "data", "gold",   "dim_cards")
    gold_merchant = os.path.join(base_dir, "data", "gold",   "dim_merchant")
    gold_fact_out = os.path.join(base_dir, "data", "gold",   "fact_transactions")

    spark = SparkSession.builder \
        .appName("DWH_05_Build_fact_transactions") \
        .master("local[*]") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # ── Đọc Silver fact ──
    print(f"-> Đọc fact_transactions Silver từ: {silver_fact}")
    fact = spark.read.parquet(silver_fact)
    if args.limit > 0:
        print(f"-> Giới hạn xử lý: {args.limit:,} dòng")
        fact = fact.limit(args.limit)
    print(f"-> Schema Silver fact: {fact.columns}")

    # ── Đọc các Dim Gold ĐẦY ĐỦ PHIÊN BẢN (Point-in-Time Join) ──
    # KHÔNG lọc is_current=True để có thể join đúng phiên bản tại thời điểm giao dịch phát sinh!
    d_user = spark.read.parquet(gold_users).select(
        col("user_id").alias("u_user_id"),
        col("user_sk"),
        col("effective_date").alias("u_eff_date"),
        col("expiration_date").alias("u_exp_date"),
        col("is_current").alias("u_is_current")
    )

    d_card = spark.read.parquet(gold_cards).select(
        col("user_id").alias("c_user_id"),
        col("card_index").alias("c_card_index"),
        col("card_sk"),
        col("effective_date").alias("c_eff_date"),
        col("expiration_date").alias("c_exp_date"),
        col("is_current").alias("c_is_current")
    )

    d_merchant = spark.read.parquet(gold_merchant).select(
        "merchant_sk", "merchant_name", "merchant_city", "merchant_state", "mcc"
    )

    # ── Phát hiện tên cột trong Silver fact ──
    uid_col  = next((c for c in fact.columns if c.lower() in ["user_id", "person", "user"]), None)
    cidx_col = next((c for c in fact.columns if c.lower() in ["card_id", "card_index", "card"]), None)
    ts_col   = next((c for c in fact.columns if "time" in c.lower() or "timestamp" in c.lower()), None) or "transaction_timestamp"

    print(f"-> Mapping: user_col={uid_col}, card_col={cidx_col}, ts_col={ts_col}")

    # ── Thêm date_sk và txn_date (YYYY-MM-DD) phục vụ SCD2 Point-in-Time Join ──
    fact_prep = fact \
        .withColumn("date_sk", date_format(col(ts_col), "yyyyMMdd").cast(IntegerType())) \
        .withColumn("txn_date", date_format(col(ts_col), "yyyy-MM-dd"))

    if uid_col:
        fact_prep = fact_prep.withColumn("_uid", col(uid_col).cast("long"))
    else:
        fact_prep = fact_prep.withColumn("_uid", lit(None).cast("long"))

    if cidx_col:
        fact_prep = fact_prep.withColumn("_cidx", col(cidx_col).cast("integer"))
    else:
        fact_prep = fact_prep.withColumn("_cidx", lit(None).cast("integer"))

    # ── Sinh degenerate transaction_id deterministic (Idempotent 100%) ──
    # Băm MD5 từ toàn bộ các thuộc tính tự nhiên của giao dịch để bảo đảm 100% IDEMPOTENT
    # Tuyệt đối không dùng Window orderBy(lit(1)) hay monotonically_increasing_id() vì gây bất định giữa các lần chạy
    merch_name_col  = next((c for c in fact.columns if "merchant" in c.lower() and "name"  in c.lower()), None)
    merch_city_col  = next((c for c in fact.columns if "merchant" in c.lower() and "city"  in c.lower()), None)
    merch_state_col = next((c for c in fact.columns if "merchant" in c.lower() and "state" in c.lower()), None)
    mcc_col         = next((c for c in fact.columns if c.lower() == "mcc"), None)
    amount_col      = next((c for c in fact.columns if c.lower() == "amount"), None)
    fraud_col       = next((c for c in fact.columns if "fraud" in c.lower()), None)
    chip_col        = next((c for c in fact.columns if "chip" in c.lower() or "use_chip" in c.lower()), None)

    txn_id_col = next((c for c in fact.columns if "transaction" in c.lower() and "id" in c.lower()), None)
    if not txn_id_col:
        fact_prep = fact_prep.withColumn(
            "transaction_id",
            md5(concat_ws("|",
                col("_uid").cast("string"),
                col("_cidx").cast("string"),
                col(ts_col).cast("string"),
                col(amount_col).cast("string") if amount_col else lit("0"),
                col(merch_name_col).cast("string") if merch_name_col else lit(""),
                col(merch_city_col).cast("string") if merch_city_col else lit(""),
                col(merch_state_col).cast("string") if merch_state_col else lit(""),
                col(mcc_col).cast("string") if mcc_col else lit(""),
                col(chip_col).cast("string") if chip_col else lit("")
            ))
        )
        txn_id_col = "transaction_id"

    # ── SCD2 POINT-IN-TIME JOIN: fact → dim_users ──
    # Điều kiện Kimball: effective_date <= txn_date < expiration_date
    user_pit_cond = (
        (fact_prep["_uid"] == d_user["u_user_id"]) &
        (
            (
                (fact_prep["txn_date"] >= d_user["u_eff_date"]) &
                (fact_prep["txn_date"] <  d_user["u_exp_date"])
            ) |
            (fact_prep["txn_date"].isNull() & (d_user["u_is_current"] == True))
        )
    )
    fact_with_user = fact_prep.join(d_user, user_pit_cond, how="left") \
        .withColumnRenamed("user_sk", "pit_user_sk")

    # Fallback cho user nếu txn_date nằm ngoài dải: lấy bản ghi current
    d_user_curr = d_user.filter(col("u_is_current") == True).select(
        col("u_user_id").alias("fb_u_id"),
        col("user_sk").alias("fb_user_sk")
    )
    fact_with_user = fact_with_user.join(
        d_user_curr,
        fact_with_user["_uid"] == d_user_curr["fb_u_id"],
        how="left"
    ).withColumn("user_sk", coalesce(col("pit_user_sk"), col("fb_user_sk")))

    user_pit_direct = fact_with_user.filter(col("pit_user_sk").isNotNull()).count()
    user_fb_used    = fact_with_user.filter(col("pit_user_sk").isNull() & col("user_sk").isNotNull()).count()
    fact_with_user  = fact_with_user.drop("pit_user_sk", "fb_u_id", "fb_user_sk")

    # ── SCD2 POINT-IN-TIME JOIN: fact → dim_cards ──
    # Điều kiện Kimball: effective_date <= txn_date < expiration_date
    card_pit_cond = (
        (fact_with_user["_uid"]  == d_card["c_user_id"]) &
        (fact_with_user["_cidx"] == d_card["c_card_index"]) &
        (
            (
                (fact_with_user["txn_date"] >= d_card["c_eff_date"]) &
                (fact_with_user["txn_date"] <  d_card["c_exp_date"])
            ) |
            (fact_with_user["txn_date"].isNull() & (d_card["c_is_current"] == True))
        )
    )
    fact_with_card = fact_with_user.join(d_card, card_pit_cond, how="left") \
        .withColumnRenamed("card_sk", "pit_card_sk")

    # Fallback cho card nếu txn_date nằm ngoài dải: lấy bản ghi current
    d_card_curr = d_card.filter(col("c_is_current") == True).select(
        col("c_user_id").alias("fb_c_uid"),
        col("c_card_index").alias("fb_c_cidx"),
        col("card_sk").alias("fb_card_sk")
    )
    fact_with_card = fact_with_card.join(
        d_card_curr,
        (fact_with_card["_uid"] == d_card_curr["fb_c_uid"]) &
        (fact_with_card["_cidx"] == d_card_curr["fb_c_cidx"]),
        how="left"
    ).withColumn("card_sk", coalesce(col("pit_card_sk"), col("fb_card_sk")))

    card_pit_direct = fact_with_card.filter(col("pit_card_sk").isNotNull()).count()
    card_fb_used    = fact_with_card.filter(col("pit_card_sk").isNull() & col("card_sk").isNotNull()).count()
    fact_with_card  = fact_with_card.drop("pit_card_sk", "fb_c_uid", "fb_c_cidx", "fb_card_sk")

    # ── JOIN: fact → dim_merchant (SCD Type 1) ──
    if merch_name_col:
        fact_gold = fact_with_card.join(
            d_merchant,
            (fact_with_card[merch_name_col]  == d_merchant["merchant_name"])  &
            (fact_with_card[merch_city_col]  == d_merchant["merchant_city"])  &
            (fact_with_card[merch_state_col] == d_merchant["merchant_state"]) &
            (fact_with_card[mcc_col].cast("string") == d_merchant["mcc"]),
            how="left"
        )
    else:
        fact_gold = fact_with_card.withColumn("merchant_sk", lit(None).cast("long"))

    # ── Select final Star Schema fact columns ──
    select_list = [
        col("transaction_id"),
        col("date_sk"),
        col("user_sk"),
        col("card_sk"),
        col("merchant_sk")
    ]

    if amount_col:
        select_list.append(col("amount"))
    if fraud_col:
        select_list.append(col("is_fraud"))
    if chip_col:
        select_list.append(col("use_chip"))

    select_list.append(col(ts_col).alias("transaction_timestamp"))

    fact_final = fact_gold.select(*select_list)

    row_count = fact_final.count()
    print(f"-> Tổng số giao dịch (Atomic Grain): {row_count:,}")

    # Minh bạch hóa kiểm tra Fallback Coalesce (Point 6)
    print(f"-> Thống kê PIT Join dim_users : Khớp trực tiếp = {user_pit_direct:,} | Dùng Fallback = {user_fb_used:,} ({user_fb_used/row_count*100:.2f}%)")
    print(f"-> Thống kê PIT Join dim_cards : Khớp trực tiếp = {card_pit_direct:,} | Dùng Fallback = {card_fb_used:,} ({card_fb_used/row_count*100:.2f}%)")

    # Orphan check trước khi ghi
    orphan_u = fact_final.filter(col("user_sk").isNull()).count()
    orphan_c = fact_final.filter(col("card_sk").isNull()).count()
    orphan_m = fact_final.filter(col("merchant_sk").isNull()).count()
    print(f"-> Kiểm tra tham chiếu: orphan user_sk={orphan_u:,}, "
          f"card_sk={orphan_c:,}, merchant_sk={orphan_m:,}")

    print(f"-> Ghi Parquet vào: {gold_fact_out}")
    fact_final.write.mode("overwrite").parquet(gold_fact_out)

    print("✅ [DWH JOB 05] DONE — fact_transactions Gold sẵn sàng (SCD2 PIT Join chuẩn mực)")
    spark.stop()


if __name__ == "__main__":
    main()
