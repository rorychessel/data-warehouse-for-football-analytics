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
        .appName("ETL_Dim_Player_V3_WithTeam") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_CONF["endpoint"]) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_CONF["access_key"]) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_CONF["secret_key"]) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

def run_etl():
    spark = get_spark_session()
    print(">>> [DIM_PLAYER] Bắt đầu xử lý (Version 3 - Có Team Name)...")

    # 1. Đọc Matches (Chỉ cần ID và Date)
    try:
        df_matches = spark.read.json("s3a://football-data/raw/matches/date={process_date}/*.json") \
            .select(
                col("match_id").cast(LongType()).alias("match_id"), 
                col("match_date")
            )
    except Exception as e:
        print(f"ERROR reading Matches: {e}")
        return

    # 2. Đọc Lineups và trích xuất Match ID
    try:
        df_lineups_raw = spark.read.json("s3a://football-data/raw/lineups/*/*.json")
        
        # Regex lấy số match_id từ tên file
        df_lineups = df_lineups_raw.withColumn(
            "match_id_extracted", 
            regexp_extract(input_file_name(), r"(\d+)\.json", 1).cast(LongType())
        )
    except Exception as e:
        print(f"ERROR reading Lineups: {e}")
        return

    # 3. Flatten Lineups
    # LƯU Ý: team_name nằm ở root của struct lineup, cùng cấp với team_id
    df_players = df_lineups.select(
        col("match_id_extracted").alias("match_id"),
        col("team_id"),
        col("team_name"), # <--- Lấy thêm Team Name ở đây
        explode("lineup").alias("p")
    ).select(
        col("match_id"),
        col("team_id"),
        col("team_name"), # <--- Giữ lại cột này
        col("p.player_id").alias("player_statsbomb_id"),
        col("p.player_name"),
        col("p.country.name").alias("nationality"),
        col("p.positions")[0].getField("position").alias("specific_position")
    )

    # 4. Join Matches để lấy Date
    df_joined = df_players.join(df_matches, on="match_id", how="inner") \
        .withColumn("match_date", to_date(col("match_date")))

    # 5. Logic SCD Type 2
    # Partition by Player, Order by Date
    window_spec = Window.partitionBy("player_statsbomb_id").orderBy("match_date")
    
    df_scd = df_joined.withColumn("prev_team", lag("team_id").over(window_spec))

    # Lọc thay đổi: Dòng đầu tiên HOẶC Team ID thay đổi (Chuyển nhượng)
    df_changes = df_scd.filter(
        (col("prev_team").isNull()) |
        (col("team_id") != col("prev_team")) 
    )

    # Tính Effective To
    window_next = Window.partitionBy("player_statsbomb_id").orderBy("match_date")
    
    df_pre_final = df_changes.withColumn("effective_from", col("match_date")) \
        .withColumn("next_start_date", lead("match_date").over(window_next)) \
        .withColumn("effective_to", 
            when(col("next_start_date").isNotNull(), date_sub(col("next_start_date"), 1))
            .otherwise(to_date(lit("9999-12-31")))
        ) \
        .withColumn("is_current", 
            when(col("effective_to") == to_date(lit("9999-12-31")), True).otherwise(False)
        ) \
        .withColumn("kaggle_name", col("player_name")) \
        .withColumn("birth_year", lit(None).cast(IntegerType()))

    # --- TẠO POSITION GROUP ---
    df_final = df_pre_final.withColumn("position_group", 
        when(col("specific_position") == "Goalkeeper", "Goalkeeper")
        .when(col("specific_position").like("%Back%"), "Defender")
        .when(col("specific_position").like("%Midfield%"), "Midfielder")
        .when(col("specific_position").like("%Wing%"), "Forward")
        .when(col("specific_position").isin("Striker", "Center Forward", "Second Striker"), "Forward")
        .otherwise("Unknown")
    )

    # 6. Select columns
    df_write = df_final.select(
        col("player_statsbomb_id").cast(StringType()),
        col("player_name"),
        col("kaggle_name"),
        col("nationality"),
        col("position_group"),
        col("specific_position"),
        col("team_name"),       # <--- Ghi cột Team Name vào DB
        col("birth_year"),
        col("effective_from"),
        col("effective_to"),
        col("is_current"),
        current_timestamp().alias("created_at")
    )

    print(f">>> [DIM_PLAYER] Đang ghi {df_write.count()} dòng vào Postgres...")
    
    try:
        df_write.write.jdbc(url=POSTGRES_URL, table="dim_player", mode="append", properties=POSTGRES_PROPS)
        print(">>> [DIM_PLAYER] Thành công!")
    except Exception as e:
        print(f"ERROR writing dim_player: {e}")

    spark.stop()

if __name__ == "__main__":
    run_etl()