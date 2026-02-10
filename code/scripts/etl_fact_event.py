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
        .appName("ETL_Fact_Event_Final_Fixed") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_CONF["endpoint"]) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_CONF["access_key"]) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_CONF["secret_key"]) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.autoBroadcastJoinThreshold", "20mb") \
        .getOrCreate()

def run_etl():
    spark = get_spark_session()
    print(">>> [FACT_EVENT] Bắt đầu xử lý (Fixing Overflow & Negative Durations)...")

    # ==========================================
    # 1. LOAD DIMENSIONS
    # ==========================================
    dim_match = spark.read.jdbc(POSTGRES_URL, "dim_match", properties=POSTGRES_PROPS) \
        .select("match_sk", "match_statsbomb_id", "match_date")

    dim_team = spark.read.jdbc(POSTGRES_URL, "dim_team", properties=POSTGRES_PROPS) \
        .select("team_sk", "team_natural_id", "effective_from", "effective_to")

    dim_player = spark.read.jdbc(POSTGRES_URL, "dim_player", properties=POSTGRES_PROPS) \
        .select("player_sk", "player_statsbomb_id", "effective_from", "effective_to")

    dim_event_type = spark.read.jdbc(POSTGRES_URL, "dim_event_type", properties=POSTGRES_PROPS) \
        .select("event_type_sk", "event_type", "outcome")

    dim_play_pattern = spark.read.jdbc(POSTGRES_URL, "dim_play_pattern", properties=POSTGRES_PROPS) \
        .select("play_pattern_sk", "play_pattern_id")

    # ==========================================
    # 2. READ RAW EVENTS
    # ==========================================
    try:
        df_raw = spark.read.json("s3a://football-data/raw/events/date={process_date}/*/*.json")
        df_events = df_raw.withColumn(
            "match_id_extracted", 
            regexp_extract(input_file_name(), r"(\d+)\.json", 1)
        )
    except Exception as e:
        print(f"ERROR reading events: {e}")
        return

    # ==========================================
    # 3. TRANSFORM
    # ==========================================
    
    # 3.1 Dynamic Outcome extraction
    outcome_columns = []
    for field in df_raw.schema.fields:
        if isinstance(field.dataType, StructType) and 'outcome' in field.dataType.names:
            outcome_columns.append(col(f"{field.name}.outcome.name"))
    final_outcome_col = coalesce(*outcome_columns) if outcome_columns else lit(None)

    # 3.2 End Location (Coalesce Pass, Shot, Carry, Goalkeeper)
    end_loc_x = coalesce(
        col("pass.end_location").getItem(0), 
        col("shot.end_location").getItem(0), 
        col("carry.end_location").getItem(0),
        col("goalkeeper.end_location").getItem(0)
    )
    end_loc_y = coalesce(
        col("pass.end_location").getItem(1), 
        col("shot.end_location").getItem(1), 
        col("carry.end_location").getItem(1),
        col("goalkeeper.end_location").getItem(1)
    )

    # 3.3 Initial Selection and Data Cleaning (Handling negative/invalid duration)
    df_trans = df_events.select(
        col("id").alias("statsbomb_event_id"),
        col("match_id_extracted").alias("match_statsbomb_id"),
        col("team.id").cast(StringType()).alias("team_natural_id"),
        col("player.id").cast(StringType()).alias("player_statsbomb_id"),
        col("type.name").alias("event_type_name"),
        final_outcome_col.alias("outcome_name"),
        col("play_pattern.id").alias("play_pattern_original_id"),
        col("location").getItem(0).alias("location_x"),
        col("location").getItem(1).alias("location_y"),
        end_loc_x.alias("end_location_x"),
        end_loc_y.alias("end_location_y"),
        col("period"),
        col("minute"),
        col("second"),
        col("timestamp").alias("event_relative_time_str"), 
        
        # XỬ LÝ DURATION: Loại bỏ giá trị âm (noise) và giá trị cực đoan (> 10000s)
        when((col("duration") < 0) | (col("duration") > 10000), lit(None))
            .otherwise(col("duration")).alias("duration_raw"),
            
        col("under_pressure"),
        col("pass.length").alias("pass_length"),
        col("pass.angle").alias("pass_angle"),
        col("shot.statsbomb_xg").alias("shot_xg"),
        col("possession").cast(StringType()).alias("possession_id")
    )

    # 3.4 Zone ID Calculation
    def calculate_zone_id(x_col, y_col):
        return when((col(x_col) >= 102) & (col(y_col) >= 18) & (col(y_col) <= 62), 19) \
               .when((col(x_col) < 20), \
                    when(col(y_col) < 26.6, 1).when(col(y_col) < 53.3, 2).otherwise(3)) \
               .when((col(x_col) < 40), \
                    when(col(y_col) < 26.6, 4).when(col(y_col) < 53.3, 5).otherwise(6)) \
               .when((col(x_col) < 60), \
                    when(col(y_col) < 26.6, 7).when(col(y_col) < 53.3, 8).otherwise(9)) \
               .when((col(x_col) < 80), \
                    when(col(y_col) < 26.6, 10).when(col(y_col) < 53.3, 11).otherwise(12)) \
               .when((col(x_col) < 100), \
                    when(col(y_col) < 26.6, 13).when(col(y_col) < 53.3, 14).otherwise(15)) \
               .otherwise( \
                    when(col(y_col) < 26.6, 16).when(col(y_col) < 53.3, 17).otherwise(18))

    df_calculated = df_trans \
        .withColumn("location_id", calculate_zone_id("location_x", "location_y")) \
        .withColumn("end_location_id", calculate_zone_id("end_location_x", "end_location_y"))

    # ==========================================
    # 4. JOIN LOOKUP
    # ==========================================
    
    # Match
    df_j_match = df_calculated.join(
        dim_match, 
        df_calculated.match_statsbomb_id == dim_match.match_statsbomb_id, 
        "inner"
    ).select(df_calculated["*"], dim_match["match_sk"], dim_match["match_date"])

    # Team (SCD2)
    cond_team = (
        (df_j_match.team_natural_id == dim_team.team_natural_id) &
        (df_j_match.match_date >= dim_team.effective_from) &
        (df_j_match.match_date <= dim_team.effective_to)
    )
    df_j_team = df_j_match.join(dim_team, cond_team, "left").select(df_j_match["*"], dim_team["team_sk"])

    # Player (SCD2)
    cond_player = (
        (df_j_match.player_statsbomb_id == dim_player.player_statsbomb_id) &
        (df_j_match.match_date >= dim_player.effective_from) &
        (df_j_match.match_date <= dim_player.effective_to)
    )
    df_j_player = df_j_team.join(dim_player, cond_player, "left").select(df_j_team["*"], dim_player["player_sk"])

    # Type & Pattern (Broadcast)
    cond_type = (
        (df_j_player.event_type_name == dim_event_type.event_type) &
        (df_j_player.outcome_name.eqNullSafe(dim_event_type.outcome))
    )
    df_j_type = df_j_player.join(broadcast(dim_event_type), cond_type, "left") \
        .select(df_j_player["*"], dim_event_type["event_type_sk"])

    df_final_join = df_j_type.join(
        broadcast(dim_play_pattern),
        df_j_type.play_pattern_original_id == dim_play_pattern.play_pattern_id,
        "left"
    ).select(df_j_type["*"], dim_play_pattern["play_pattern_sk"])

    # ==========================================
    # 5. FINAL CALCULATION & WRITE
    # ==========================================
    
    # Xử lý thời gian chính xác: HH:mm:ss.SSS -> Seconds (bao gồm cả Giờ)
    df_ready = df_final_join \
        .withColumn("date_id", date_format(col("match_date"), "yyyyMMdd").cast(IntegerType())) \
        .withColumn("t_parts", split(col("event_relative_time_str"), ":")) \
        .withColumn("calc_seconds", 
            col("t_parts")[0].cast("float") * 3600 + 
            col("t_parts")[1].cast("float") * 60 + 
            col("t_parts")[2].cast("float")
        ) \
        .withColumn("event_relative_time", 
            when(col("calc_seconds") > 18000, lit(None)).otherwise(col("calc_seconds"))
        )

    # Cast về schema Database, dùng Decimal(10,3) để khớp với ALTER TABLE
    df_write = df_ready.select(
        col("statsbomb_event_id"),
        col("date_id"),
        col("match_sk"),
        col("team_sk"),
        col("player_sk"),
        col("event_type_sk"),
        col("play_pattern_sk"),
        col("location_id"),
        col("end_location_id"),
        col("location_x").cast("decimal(5,2)"),
        col("location_y").cast("decimal(5,2)"),
        col("end_location_x").cast("decimal(5,2)"),
        col("end_location_y").cast("decimal(5,2)"),
        col("period"),
        col("minute"),
        col("second"),
        col("event_relative_time").cast("decimal(10,3)"),
        col("duration_raw").cast("decimal(10,3)").alias("duration"),
        col("under_pressure"),
        col("pass_length").cast("decimal(6,2)"),
        col("pass_angle").cast("decimal(5,4)"),
        col("shot_xg").cast("decimal(5,4)"),
        col("possession_id"),
        current_timestamp().alias("created_at")
    ).filter(col("date_id").isNotNull())

    print(f">>> [FACT_EVENT] Writing events to Postgres...")
    
    df_write.write.jdbc(
        url=POSTGRES_URL, 
        table="fact_event", 
        mode="append", 
        properties=POSTGRES_PROPS
    )
    print(">>> [FACT_EVENT] Success!")
    spark.stop()

if __name__ == "__main__":
    run_etl()