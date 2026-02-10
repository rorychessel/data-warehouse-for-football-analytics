from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys

# --- CONFIG ---
MINIO_CONF = {
    "endpoint": "http://minio:9000",
    "access_key": "minio_admin",
    "secret_key": "minio_password"
}

POSTGRES_PROPS = {
    "user": "admin",
    "password": "root",
    "driver": "org.postgresql.Driver"
}
POSTGRES_URL = "jdbc:postgresql://warehouse_db:5432/football_dw"

process_date = sys.argv[sys.argv.index("--process_date") + 1]

def get_spark_session():
    return SparkSession.builder \
        .appName("ETL_Dim_Play_Pattern") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_CONF["endpoint"]) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_CONF["access_key"]) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_CONF["secret_key"]) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

def run_etl():
    spark = get_spark_session()
    print(">>> [DIM_PLAY_PATTERN] Đang xử lý...")

    # 1. Đọc dữ liệu Events
    # Chỉ cần cột play_pattern
    try:
        df_events = spark.read.json("s3a://football-data/raw/events/date={process_date}/*/*.json")
    except Exception as e:
        print(f"ERROR reading Events: {e}")
        return

    # 2. Select và Distinct
    # Lấy các cặp ID và Name duy nhất
    df_pattern = df_events.select(
        col("play_pattern.id").cast(IntegerType()).alias("play_pattern_id"),
        col("play_pattern.name").alias("play_pattern_name")
    ).filter(col("play_pattern_id").isNotNull()) \
     .dropDuplicates(["play_pattern_id"]) # Đảm bảo mỗi ID chỉ xuất hiện 1 lần

    # 3. Thêm cột created_at
    df_write = df_pattern.withColumn("created_at", current_timestamp())

    # 4. Ghi vào Postgres
    print(f">>> [DIM_PLAY_PATTERN] Tìm thấy {df_write.count()} patterns. Đang ghi...")
    
    try:
        df_write.write.jdbc(url=POSTGRES_URL, table="dim_play_pattern", mode="append", properties=POSTGRES_PROPS)
        print(">>> [DIM_PLAY_PATTERN] Thành công!")
    except Exception as e:
        print(f"ERROR writing to DB: {e}")

    spark.stop()

if __name__ == "__main__":
    run_etl()