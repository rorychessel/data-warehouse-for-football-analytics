from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
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
        .appName("ETL_Dim_Team_V2") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_CONF["endpoint"]) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_CONF["access_key"]) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_CONF["secret_key"]) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

def run_etl():
    spark = get_spark_session()
    print(">>> [DIM_TEAM] Bắt đầu xử lý...")

    # 1. Đọc dữ liệu Matches
    # Schema đọc nhanh
    try:
        df_matches = spark.read.json("s3a://football-data/raw/matches/date={process_date}/*.json")
    except Exception as e:
        print(f"ERROR reading Matches: {e}")
        return

    # 2. Transform: Tách Home và Away
    # Manager: Lấy tên người đầu tiên trong mảng managers
    
    # --- Home Team ---
    df_home = df_matches.select(
        col("home_team.home_team_id").alias("team_natural_id"),
        col("home_team.home_team_name").alias("team_name"),
        col("home_team.managers")[0].getField("name").alias("manager_name"),
        col("stadium.name").alias("home_stadium"),
        col("match_date")
    )

    # --- Away Team ---
    # Away team không lấy stadium của trận đấu gán cho mình (để null)
    df_away = df_matches.select(
        col("away_team.away_team_id").alias("team_natural_id"),
        col("away_team.away_team_name").alias("team_name"),
        col("away_team.managers")[0].getField("name").alias("manager_name"),
        lit(None).cast(StringType()).alias("home_stadium"), 
        col("match_date")
    )

    # Gộp lại
    df_full = df_home.unionByName(df_away) \
        .withColumn("match_date", to_date(col("match_date"))) \
        .filter(col("team_natural_id").isNotNull())

    # 3. Logic SCD Type 2
    # Partition theo Team ID, sắp xếp theo ngày đá
    window_spec = Window.partitionBy("team_natural_id").orderBy("match_date")
    
    # Lấy giá trị của dòng trước đó để so sánh
    df_scd = df_full.withColumn("prev_manager", lag("manager_name").over(window_spec)) \
                    .withColumn("prev_name", lag("team_name").over(window_spec)) \
                    .withColumn("prev_stadium", lag("home_stadium").over(window_spec))

    # Xác định có thay đổi hay không?
    # Logic: Dòng đầu tiên (prev is null) HOẶC manager thay đổi HOẶC tên đội thay đổi
    # Chỉ SCD khi đổi HLV hoặc đổi tên. Stadium lấy theo dòng hiện tại nếu có.
    
    df_changes = df_scd.filter(
        (col("prev_manager").isNull()) | 
        (col("manager_name") != col("prev_manager")) |
        (col("team_name") != col("prev_name"))
    )

    # Tính Effective To
    window_next = Window.partitionBy("team_natural_id").orderBy("match_date")
    
    df_final = df_changes.withColumn("effective_from", col("match_date")) \
        .withColumn("next_start_date", lead("match_date").over(window_next)) \
        .withColumn("effective_to", 
            when(col("next_start_date").isNotNull(), date_sub(col("next_start_date"), 1))
            .otherwise(to_date(lit("9999-12-31")))
        ) \
        .withColumn("is_current", 
            when(col("effective_to") == to_date(lit("9999-12-31")), True).otherwise(False)
        ) \
        .withColumn("kaggle_team_name", col("team_name")) # Mapping tạm

    # 4. Select columns khớp DDL Postgres
    # Bỏ qua team_sk (Identity tự sinh)
    df_write = df_final.select(
        col("team_natural_id").cast(StringType()),
        col("kaggle_team_name"),
        col("team_name"),
        col("manager_name"),
        col("home_stadium"),
        col("effective_from"),
        col("effective_to"),
        col("is_current"),
        current_timestamp().alias("created_at")
    )

    print(f">>> [DIM_TEAM] Đang ghi {df_write.count()} dòng vào Postgres...")
    
    try:
        df_write.write.jdbc(url=POSTGRES_URL, table="dim_team", mode="append", properties=POSTGRES_PROPS)
        print(">>> [DIM_TEAM] Thành công!")
    except Exception as e:
        print(f"ERROR writing dim_team: {e}")

    spark.stop()

if __name__ == "__main__":
    run_etl()