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

def get_spark_session():
    return SparkSession.builder \
        .appName("ETL_Dim_Match_Trimmed") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_CONF["endpoint"]) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_CONF["access_key"]) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_CONF["secret_key"]) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

def run_etl():
    spark = get_spark_session()
    print(">>> [DIM_MATCH] Bắt đầu xử lý...")

    # 1. Đọc dữ liệu Matches từ MinIO
    try:
        # Đọc tất cả file json trong thư mục matches
        df_matches_raw = spark.read.json("s3a://football-data/raw/matches/date={process_date}/*.json")
    except Exception as e:
        print(f"ERROR reading matches: {e}")
        return

    # Select và ép kiểu, thêm hàm TRIM cho stadium
    df_matches = df_matches_raw.select(
        col("match_id").cast(StringType()).alias("match_statsbomb_id"),
        col("match_date").cast(DateType()),
        to_timestamp(concat(col("match_date"), lit(" "), col("kick_off"))).alias("kickoff_time"),
        col("season.season_name").alias("season"),
        col("competition.competition_name").alias("competition"),
        col("home_team.home_team_id").cast(StringType()).alias("home_team_natural_id"),
        col("away_team.away_team_id").cast(StringType()).alias("away_team_natural_id"),
        col("home_score"),
        col("away_score"),
        
        # --- SỬA LỖI DƯ DẤU CÁCH Ở ĐÂY ---
        trim(col("stadium.name")).alias("stadium"), 
        
        col("referee.name").alias("referee"),
        col("match_week").cast(IntegerType()).alias("round"),
        col("competition_stage.name").alias("season_stage")
    )

    # 2. Đọc bảng dim_team từ Postgres để Lookup SK
    print(">>> Reading dim_team from Postgres for Lookup...")
    try:
        df_dim_team = spark.read.jdbc(url=POSTGRES_URL, table="dim_team", properties=POSTGRES_PROPS) \
            .select(
                col("team_sk"),
                col("team_natural_id"),
                col("effective_from"),
                col("effective_to")
            )
    except Exception as e:
        print(f"ERROR reading dim_team: {e}")
        return

    # 3. Join lấy Home Team SK & Away Team SK
    dt_home = df_dim_team.alias("dt_home")
    dt_away = df_dim_team.alias("dt_away")

    # Join Home
    cond_home = (
        (col("home_team_natural_id") == col("dt_home.team_natural_id")) & 
        (col("match_date") >= col("dt_home.effective_from")) & 
        (col("match_date") <= col("dt_home.effective_to"))
    )
    
    df_w_home = df_matches.join(dt_home, cond_home, "left") \
        .select(df_matches["*"], col("dt_home.team_sk").alias("home_team_sk"))

    # Join Away
    cond_away = (
        (col("away_team_natural_id") == col("dt_away.team_natural_id")) & 
        (col("match_date") >= col("dt_away.effective_from")) & 
        (col("match_date") <= col("dt_away.effective_to"))
    )
    
    df_full = df_w_home.join(dt_away, cond_away, "left") \
        .select(df_w_home["*"], col("dt_away.team_sk").alias("away_team_sk"))

    # 4. Thêm các cột metadata
    df_final = df_full.withColumn("effective_from", col("match_date")) \
        .withColumn("effective_to", to_date(lit("9999-12-31"))) \
        .withColumn("is_current", lit(True)) \
        .withColumn("created_at", current_timestamp())

    # 5. Select và Ghi vào DB
    df_write = df_final.select(
        col("match_statsbomb_id"),
        col("season"),
        col("competition"),
        col("match_date"),
        col("kickoff_time"),
        col("home_team_sk"),
        col("away_team_sk"),
        col("home_score"),
        col("away_score"),
        col("stadium"), # Cột này đã được trim
        col("referee"),
        col("round"),
        col("season_stage"),
        col("effective_from"),
        col("effective_to"),
        col("is_current"),
        col("created_at")
    )

    print(f">>> [DIM_MATCH] Writing {df_write.count()} matches to DB...")
    
    try:
        df_write.write.jdbc(url=POSTGRES_URL, table="dim_match", mode="append", properties=POSTGRES_PROPS)
        print(">>> [DIM_MATCH] Success!")
    except Exception as e:
        print(f"!!! ERROR: {str(e)}")

    spark.stop()

if __name__ == "__main__":
    run_etl()