from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# --- CONFIG ---
MINIO_CONF = {
    "endpoint": "http://minio:9000",
    "access_key": "minio_admin",
    "secret_key": "minio_password"
}
POSTGRES_PROPS = { "user": "admin", "password": "root", "driver": "org.postgresql.Driver" }
POSTGRES_URL = "jdbc:postgresql://warehouse_db:5432/football_dw"

def run_etl():
    spark = SparkSession.builder \
        .appName("ETL_Dim_Location_Gen") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_CONF["endpoint"]) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_CONF["access_key"]) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_CONF["secret_key"]) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()
    
    print(">>> [DIM_LOCATION] Generating Data...")

    # --- ĐỊNH NGHĨA DỮ LIỆU THỦ CÔNG ---
    # Chúng ta tạo ra 19 zones: 18 Zones cơ bản + 1 Penalty Box (ưu tiên)
    
    data = [
        # --- DEFENSIVE THIRD (0-40 yards) ---
        (1, "Zone 1 (Defensive Left)", False),
        (2, "Zone 2 (Defensive Center-Left)", False),
        (3, "Zone 3 (Defensive Center-Right)", False),
        (4, "Zone 4 (Defensive Right)", False),
        
        # --- MIDDLE THIRD (40-80 yards) ---
        (5, "Zone 5 (Midfield Left)", False),
        (6, "Zone 6 (Midfield Center-Left)", False),
        (7, "Zone 7 (Midfield Center-Right)", False),
        (8, "Zone 8 (Midfield Right)", False),
        
        # --- ATTACKING THIRD (80-120 yards) ---
        (9,  "Zone 9 (Attacking Left Wing)", False),
        (10, "Zone 10 (Attacking Half-Space Left)", False),
        (11, "Zone 11 (Attacking Center)", False),
        (12, "Zone 12 (Attacking Half-Space Right)", False),
        (13, "Zone 13 (Attacking Right Wing)", False),

        # --- KHU VỰC ĐẶC BIỆT ---
        (14, "Zone 14 (The Hole / CAM Zone)", False), # Zone quan trọng nhất
        (15, "Zone 15 (Deep Zone Left)", False),
        (16, "Zone 16 (Deep Zone Center)", True), # Gần gôn nhất
        (17, "Zone 17 (Deep Zone Right)", False),
        (18, "Zone 18 (Corner Area)", False),
        
        # --- RIÊNG BIỆT ---
        (19, "Penalty Box", True)
    ]

    # Tạo DataFrame từ list
    schema = StructType([
        StructField("temp_id", IntegerType(), False), # Chỉ dùng để sort hoặc map
        StructField("field_zone", StringType(), True),
        StructField("is_box", BooleanType(), True)
    ])
    
    df_locations = spark.createDataFrame(data, schema)

    # Thêm timestamp
    df_final = df_locations.withColumn("created_at", current_timestamp()) \
                           .drop("temp_id") # Bỏ ID tạm, để Postgres tự sinh Serial ID

    print(f">>> [DIM_LOCATION] Writing {df_final.count()} zones to DB...")
    
    try:
        df_final.write.jdbc(url=POSTGRES_URL, table="dim_location", mode="append", properties=POSTGRES_PROPS)
        print(">>> [DIM_LOCATION] Success!")
        
    except Exception as e:
        print(f"!!! ERROR: {str(e)}")

    spark.stop()

if __name__ == "__main__":
    run_etl()