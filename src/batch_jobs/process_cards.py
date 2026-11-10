import os
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, regexp_replace, when, upper, trim, to_date, concat, lit
)

# Add src to sys.path to import internal tools
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.tools.etl_audit_quarantine import ETLAuditLogger, route_to_quarantine


def main():
    start_time = datetime.now()
    print("=== [BATCH JOB] BẮT ĐẦU XỬ LÝ DỮ LIỆU BẢNG DIM_CARDS ===")
    
    # KHỞI TẠO SPARK SESSION
    spark = SparkSession.builder \
        .appName("ProcessCardsBatch") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    # XÁC ĐỊNH ĐƯỜNG DẪN FILE ĐẦU VÀO VÀ ĐẦU RA
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    input_path = os.path.join(base_dir, "data", "bronze", "sd254_cards.csv")
    output_path = os.path.join(base_dir, "data", "silver", "dim_cards")

    print(f"-> Đọc dữ liệu thẻ tín dụng thô từ: {input_path}")
    
    # Đọc file CSV thô từ tầng Bronze
    df_raw = spark.read.csv(input_path, header=True, inferSchema=True)

    # LÀM SẠCH VÀ CHUẨN HÓA CÁC CỘT DỮ LIỆU THẺ
    # - Xóa ký tự $ trong Hạn mức tín dụng (credit_limit)
    # - Chuẩn hóa cột Boolean (Has Chip, Card on Dark Web thành True/False)
    # - Đổi định dạng ngày tháng MM/YYYY thành chuẩn DateType (YYYY-MM-DD)
    df_clean = df_raw \
        .withColumn("credit_limit", regexp_replace(col("Credit Limit"), r"[\$,]", "").cast("double")) \
        .withColumn("has_chip", when(upper(trim(col("Has Chip"))) == "YES", True).otherwise(False)) \
        .withColumn("card_on_dark_web", when(upper(trim(col("Card on Dark Web"))) == "YES", True).otherwise(False)) \
        .withColumn("acct_open_date", to_date(concat(trim(col("Acct Open Date")), lit("/01")), "MM/yyyy/dd")) \
        .withColumn("expires", to_date(concat(trim(col("Expires")), lit("/01")), "MM/yyyy/dd"))

    # - Đổi tên các cột còn lại sang chuẩn snake_case
    df_clean = df_clean \
        .withColumnRenamed("User", "user_id") \
        .withColumnRenamed("CARD INDEX", "card_index") \
        .withColumnRenamed("Card Brand", "card_brand") \
        .withColumnRenamed("Card Type", "card_type") \
        .withColumnRenamed("Card Number", "card_number") \
        .withColumnRenamed("CVV", "cvv") \
        .withColumnRenamed("Cards Issued", "cards_issued") \
        .withColumnRenamed("Year PIN last Changed", "year_pin_last_changed")

    # - Ép kiểu dữ liệu chuẩn (Long, Integer, String)
    df_clean = df_clean \
        .withColumn("user_id", col("user_id").cast("long")) \
        .withColumn("card_index", col("card_index").cast("integer")) \
        .withColumn("card_number", col("card_number").cast("string")) \
        .withColumn("cvv", col("cvv").cast("string")) \
        .withColumn("cards_issued", col("cards_issued").cast("integer")) \
        .withColumn("year_pin_last_changed", col("year_pin_last_changed").cast("integer"))

    # Select sắp xếp lại thứ tự cột chuẩn
    final_cols = [
        "user_id", "card_index", "card_brand", "card_type", "card_number",
        "expires", "cvv", "has_chip", "cards_issued", "credit_limit",
        "acct_open_date", "year_pin_last_changed", "card_on_dark_web"
    ]
    df_prepared = df_clean.select(*final_cols)

    # ── QUARANTINE ROUTING (CÁCH LY DỮ LIỆU LỖI) ──
    # Ràng buộc hợp lệ: user_id & card_index không NULL, hạn mức tín dụng >= 0 (bao gồm thẻ ghi nợ hạn mức 0)
    valid_cond = (
        col("user_id").isNotNull() &
        col("card_index").isNotNull() &
        (col("credit_limit") >= 0)
    )
    reason_expr = (
        when(col("user_id").isNull(), "MISSING_USER_ID")
        .when(col("card_index").isNull(), "MISSING_CARD_INDEX")
        .when(col("credit_limit").isNull() | (col("credit_limit") < 0), "INVALID_OR_NEGATIVE_CREDIT_LIMIT")
        .otherwise("UNSPECIFIED_CARD_VALIDATION_ERROR")
    )

    df_final, df_quarantine, rows_read, rows_clean, rows_quarantined = route_to_quarantine(
        spark=spark,
        df_raw=df_prepared,
        valid_condition=valid_cond,
        rejection_reason_col=reason_expr,
        dataset_name="dim_cards",
        output_silver_dir=output_path
    )

    # GHI DỮ LIỆU ĐÃ LÀM SẠCH VÀO TẦNG SILVER (PARQUET)
    print(f"-> Tổng số dòng thẻ tín dụng sạch: {rows_clean:,}")
    print(f"-> Đang ghi file Parquet chuẩn nén Snappy vào: {output_path}")
    
    import shutil
    if os.path.exists(output_path):
        shutil.rmtree(output_path, ignore_errors=True)

    df_final.write.mode("overwrite").parquet(output_path)
    
    # ── ETL AUDIT LOGGING (GHI NHẬT KÝ KIỂM TOÁN VÀ ĐỐI SOÁT) ──
    end_time = datetime.now()
    ETLAuditLogger.log_job(
        spark=spark,
        job_name="process_cards",
        pipeline_stage="Bronze_to_Silver",
        source_path=input_path,
        target_path=output_path,
        rows_read=rows_read,
        rows_clean=rows_clean,
        rows_quarantined=rows_quarantined,
        start_time=start_time,
        end_time=end_time,
        status="SUCCESS"
    )

    print("✅ [BATCH JOB] process_cards DONE")
    spark.stop()


if __name__ == "__main__":
    main()
