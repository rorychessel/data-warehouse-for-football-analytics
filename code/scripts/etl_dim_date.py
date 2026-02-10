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
POSTGRES_PROPS = { "user": "admin", "password": "root", "driver": "org.postgresql.Driver" }
POSTGRES_URL = "jdbc:postgresql://warehouse_db:5432/football_dw"

process_date = sys.argv[sys.argv.index("--process_date") + 1]

def run_etl():
    spark = SparkSession.builder \
        .appName("ETL_Dim_Date_Fixed") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_CONF["endpoint"]) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_CONF["access_key"]) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_CONF["secret_key"]) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()
    
    print(">>> [DIM_DATE] Scanning Match Dates from Source...")

    try:
        # 1. Đọc dữ liệu Matches từ MinIO
        df_matches = spark.read.json("s3a://football-data/raw/matches/date={process_date}/*.json")
        
        # 2. Lấy danh sách ngày duy nhất (DISTINCT)
        df_distinct_dates = df_matches.select(
            col("match_date").cast(DateType()).alias("date_value")
        ).distinct()

        # 3. Phân rã thông tin
        df_final = df_distinct_dates.select(
            col("date_value"),
            year("date_value").alias("year"),
            month("date_value").alias("month"),
            dayofmonth("date_value").alias("day"),
            dayofweek("date_value").alias("day_of_week")
        ).withColumn("is_weekend", 
            when(col("day_of_week").isin(1, 7), True).otherwise(False)
        ).withColumn("date_id", 
            # Smart Key: YYYYMMDD
            format_string("%d%02d%02d", col("year"), col("month"), col("day")).cast(IntegerType())
        ).drop("day_of_week") \
         .orderBy("date_value")

        df_write = df_final.select(
            "date_id", "date_value", "year", "month", "day", "is_weekend"
        )
        
        count = df_write.count()
        print(f">>> [DIM_DATE] Found {count} active match days.")

        # 4. Ghi vào Postgres
        print(">>> Writing to Postgres...")
        df_write.write.jdbc(url=POSTGRES_URL, table="dim_date", mode="append", properties=POSTGRES_PROPS)
        print(">>> [DIM_DATE] Success!")

    except Exception as e:
        print(f"!!! ERROR: {str(e)}")

    spark.stop()

if __name__ == "__main__":
    run_etl()