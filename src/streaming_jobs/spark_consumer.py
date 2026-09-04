import os
import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, concat_ws, lpad, when, upper, trim, coalesce, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, DoubleType
)

def main():    
    #CẤU HÌNH ĐƯỜNG DẪN VÀ THÔNG SỐ KAFKA BROKER
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = os.getenv("KAFKA_TOPIC", "transactions_topic")

    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    output_path = os.path.join(base_dir, "data", "silver", "fact_transactions")
    checkpoint_path = os.path.join(base_dir, "data", "silver", "checkpoints", "fact_transactions")

    print(f"-> Kafka Server  : {kafka_bootstrap}")
    print(f"-> Topic Name    : {topic}")
    print(f"-> Output Parquet: {output_path}")

    # KHỞI TẠO SPARK SESSION VỚI CẤU HÌNH PARQUET
    spark = SparkSession.builder \
        .appName("SparkKafkaConsumerFactTransactions") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    #ĐỊNH NGHĨA SCHEMA JSON GIAO DỊCH QUẸT THẺ
    json_schema = StructType([
        StructField("user_id", LongType(), True),
        StructField("card_index", IntegerType(), True),
        StructField("year", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("day", IntegerType(), True),
        StructField("time", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("use_chip", StringType(), True),
        StructField("merchant_name", StringType(), True),
        StructField("merchant_city", StringType(), True),
        StructField("merchant_state", StringType(), True),
        StructField("zip", StringType(), True),
        StructField("mcc", StringType(), True),
        StructField("errors", StringType(), True),
        StructField("is_fraud", StringType(), True),
        StructField("current_timestamp", StringType(), True),
        StructField("ingestion_timestamp", StringType(), True)
    ])

    # KẾT NỐI VÀ ĐỌC STREAM LIÊN TỤC TỪ KAFKA
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    # GIẢI MÃ JSON VÀ LÀM SẠCH DỮ LIỆU LUỒNG (MICRO-BATCH)
    # Giải mã chuỗi JSON từ Kafka Value sang DataFrame các cột
    parsed_stream = raw_stream \
        .selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), json_schema).alias("data")) \
        .select("data.*")

    # Làm sạch: Ghép chuỗi Ngày/Giờ thành Timestamp chuẩn, ép kiểu Is Fraud thành Boolean
    enriched_stream = parsed_stream \
        .withColumn(
            "transaction_timestamp",
            to_timestamp(
                concat_ws(
                    "-",
                    col("year"),
                    lpad(col("month"), 2, "0"),
                    lpad(col("day"), 2, "0"),
                    col("time")
                ),
                "yyyy-MM-dd-HH:mm"
            )
        ) \
        .withColumn("is_fraud", when(upper(trim(col("is_fraud"))) == "YES", True).otherwise(False)) \
        .withColumn("merchant_city", when(col("merchant_city") == "", "Unknown").otherwise(coalesce(col("merchant_city"), lit("Unknown")))) \
        .withColumn("merchant_state", when(col("merchant_state") == "", "Unknown").otherwise(coalesce(col("merchant_state"), lit("Unknown")))) \
        .withColumn("errors", when(col("errors") == "", "None").otherwise(coalesce(col("errors"), lit("None"))))

    #Lựa chọn các cột bảng Fact cuối cùng
    final_stream = enriched_stream.select(
        "user_id",
        "card_index",
        "transaction_timestamp",
        "amount",
        "use_chip",
        "merchant_name",
        "merchant_city",
        "merchant_state",
        "zip",
        "mcc",
        "errors",
        "is_fraud",
        "current_timestamp",
        "ingestion_timestamp"
    )

    #GHI KẾT QUẢ LIÊN TỤC VÀO TẦNG SILVER PARQUET (SINK)
    while True:
        try:
            query = final_stream.writeStream \
                .format("parquet") \
                .outputMode("append") \
                .option("path", output_path) \
                .option("checkpointLocation", checkpoint_path) \
                .trigger(processingTime="5 seconds") \
                .start()

            query.awaitTermination()
            break
        except Exception as e:
            print(f"Chờ Kafka Broker sẵn sàng kết nối... Chi tiết: {e}")
            time.sleep(3)

if __name__ == "__main__":
    main()
