from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType
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
        .appName("ETL_Dim_Event_Type_Final") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_CONF["endpoint"]) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_CONF["access_key"]) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_CONF["secret_key"]) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()
    
    print(">>> [DIM_EVENT_TYPE] Reading Data...")

    try:
        # 1. Đọc Events
        df_events = spark.read.json("s3a://football-data/raw/events/date={process_date}/*/*.json")

        # 2. LOGIC TỰ ĐỘNG TÌM CỘT OUTCOME (Dynamic)
        # Quét schema để tìm tất cả struct có chứa field con là 'outcome'
        outcome_columns = []
        print(">>> Scanning Schema for outcomes...")
        
        for field in df_events.schema.fields:
            if isinstance(field.dataType, StructType):
                if 'outcome' in field.dataType.names:
                    # Tạo column object: col("pass.outcome.name")
                    outcome_columns.append(col(f"{field.name}.outcome.name"))
        
        # Tạo cột outcome tổng hợp
        if not outcome_columns:
            final_outcome_col = lit(None)
        else:
            final_outcome_col = coalesce(*outcome_columns)

        # 3. Select dữ liệu
        df_extracted = df_events.select(
            col("type.name").alias("event_type"),
            final_outcome_col.alias("outcome")
        )

        # 4. Phân loại (Categorize)
        df_categorized = df_extracted.withColumn("event_category",
            when(col("event_type").isin("Pass", "Shot", "Dribble", "Carry", "Ball Receipt*"), "Ball Action")
            .when(col("event_type").isin("Duel", "Interception", "Clearance", "Block", "Pressure", "Ball Recovery"), "Defensive Action")
            .when(col("event_type").isin("Goal Keeper"), "Goalkeeper Action")
            .when(col("event_type").isin("Starting XI", "Substitution", "Tactical Shift"), "Tactical")
            .when(col("event_type").isin("Foul Committed", "Foul Won", "Offside"), "Foul/Offside")
            .otherwise("Other")
        )

        # 5. DISTINCT & TIMESTAMP
        # Chỉ lấy các cặp duy nhất và gán thời gian hiện tại
        df_final = df_categorized.select("event_category", "event_type", "outcome") \
            .distinct() \
            .withColumn("created_at", current_timestamp())

        print(f">>> [DIM_EVENT_TYPE] Writing {df_final.count()} unique types to DB...")
        
        # 6. Ghi vào DB
        df_final.write.jdbc(url=POSTGRES_URL, table="dim_event_type", mode="append", properties=POSTGRES_PROPS)
        print(">>> [DIM_EVENT_TYPE] Success!")

    except Exception as e:
        print(f"!!! ERROR: {str(e)}")
    
    spark.stop()

if __name__ == "__main__":
    run_etl()