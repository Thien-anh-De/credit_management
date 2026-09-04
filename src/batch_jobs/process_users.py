import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, row_number, lit
from pyspark.sql.window import Window

def main():    
    # KHỞI TẠO SPARK SESSION
    spark = SparkSession.builder \
        .appName("ProcessUsersBatch") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    # XÁC ĐỊNH ĐƯỜNG DẪN FILE ĐẦU VÀO VÀ ĐẦU RA
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    input_path = os.path.join(base_dir, "data", "bronze", "sd254_users.csv")
    output_path = os.path.join(base_dir, "data", "silver", "dim_users")

    print(f"-> Đọc dữ liệu người dùng thô từ: {input_path}")
    
    # Đọc file CSV thô từ tầng Bronze
    df_raw = spark.read.csv(input_path, header=True, inferSchema=True)

    # BƯỚC 3: LÀM SẠCH VÀ CHUẨN HÓA CÁC CỘT TIỀN TỆ & TÊN CỘT
    # - Loại bỏ ký tự $ và dấu phẩy trong các cột thu nhập, dư nợ
    df_clean = df_raw \
        .withColumn("per_capita_income", regexp_replace(col("Per Capita Income - Zipcode"), r"[\$,]", "").cast("double")) \
        .withColumn("yearly_income", regexp_replace(col("Yearly Income - Person"), r"[\$,]", "").cast("double")) \
        .withColumn("total_debt", regexp_replace(col("Total Debt"), r"[\$,]", "").cast("double"))

    # - Đổi tên cột từ tiếng Anh chuẩn thô sang dạng snake_case dễ truy vấn
    df_clean = df_clean \
        .withColumnRenamed("Person", "user_name") \
        .withColumnRenamed("Current Age", "current_age") \
        .withColumnRenamed("Retirement Age", "retirement_age") \
        .withColumnRenamed("Birth Year", "birth_year") \
        .withColumnRenamed("Birth Month", "birth_month") \
        .withColumnRenamed("Gender", "gender") \
        .withColumnRenamed("Address", "address") \
        .withColumnRenamed("Apartment", "apartment") \
        .withColumnRenamed("City", "city") \
        .withColumnRenamed("State", "state") \
        .withColumnRenamed("Zipcode", "zipcode") \
        .withColumnRenamed("Latitude", "latitude") \
        .withColumnRenamed("Longitude", "longitude") \
        .withColumnRenamed("FICO Score", "fico_score") \
        .withColumnRenamed("Num Credit Cards", "num_credit_cards")

    # - Điền giá trị mặc định cho ô trống (Null value imputation)
    df_clean = df_clean.fillna({"address": "Unknown", "apartment": "Unknown"})

    # - Ép kiểu dữ liệu chuẩn (Integer & String)
    df_clean = df_clean \
        .withColumn("current_age", col("current_age").cast("integer")) \
        .withColumn("retirement_age", col("retirement_age").cast("integer")) \
        .withColumn("birth_year", col("birth_year").cast("integer")) \
        .withColumn("birth_month", col("birth_month").cast("integer")) \
        .withColumn("zipcode", col("zipcode").cast("string")) \
        .withColumn("fico_score", col("fico_score").cast("integer")) \
        .withColumn("num_credit_cards", col("num_credit_cards").cast("integer"))

    # TẠO KHÓA CHÍNH USER_ID (BẮT ĐẦU TỪ 0)
    window_spec = Window.orderBy(lit(1))
    df_clean = df_clean.withColumn("user_id", (row_number().over(window_spec) - 1).cast("long"))

    # Select sắp xếp lại các cột theo thứ tự chuẩn
    final_cols = [
        "user_id", "user_name", "gender", "current_age", "retirement_age",
        "birth_year", "birth_month", "address", "apartment", "city", "state", "zipcode",
        "latitude", "longitude", "per_capita_income", "yearly_income", "total_debt",
        "fico_score", "num_credit_cards"
    ]
    df_final = df_clean.select(*final_cols)

    # GHI DỮ LIỆU ĐÃ LÀM SẠCH VÀO TẦNG SILVER (PARQUET)
    print(f"-> Tổng số dòng dữ liệu người dùng: {df_final.count():,}")
    print(f"-> Đang ghi file Parquet chuẩn nén Snappy vào: {output_path}")
    
    import shutil
    if os.path.exists(output_path):
        shutil.rmtree(output_path, ignore_errors=True)

    df_final.write.mode("overwrite").parquet(output_path)
    
    print(" [BATCH JOB] DONE")
    spark.stop()

if __name__ == "__main__":
    main()
