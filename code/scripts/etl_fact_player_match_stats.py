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
POSTGRES_PROPS = { "user": "admin", "password": "root", "driver": "org.postgresql.Driver" }
POSTGRES_URL = "jdbc:postgresql://warehouse_db:5432/football_dw"

process_date = sys.argv[sys.argv.index("--process_date") + 1]

def get_spark_session():
    return SparkSession.builder \
        .appName("ETL_Fact_Player_Match_Stats_Advanced") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_CONF["endpoint"]) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_CONF["access_key"]) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_CONF["secret_key"]) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .getOrCreate()

def run_etl():
    spark = get_spark_session()
    print(">>> [FACT_STATS] Bắt đầu tính toán thống kê (kèm Minutes Played & Advanced Metrics)...")

    # ==========================================
    # 1. READ RAW EVENTS
    # ==========================================
    try:
        df_raw = spark.read.json("s3a://football-data/raw/events/date={process_date}/*/*.json")
        df_events = df_raw.withColumn(
            "match_id", 
            regexp_extract(input_file_name(), r"(\d+)\.json", 1)
        )
    except Exception as e:
        print(f"ERROR reading events: {e}")
        return

    # ==========================================
    # 2. LOGIC TÍNH MINUTES PLAYED (GIỮ NGUYÊN)
    # ==========================================
    print(">>> Calculating Minutes Played...")

    # A. Tìm thời lượng trận đấu
    df_match_duration = df_events.groupBy("match_id") \
        .agg(max("minute").alias("match_end_min"))

    # B. Xác định thời điểm VÀO SÂN
    df_starters = df_events.filter(col("type.name") == "Starting XI") \
        .select("match_id", explode("tactics.lineup").alias("l")) \
        .select(
            col("match_id"), 
            col("l.player.id").cast(StringType()).alias("player_statsbomb_id"), 
            lit(0).alias("entry_min")
        )

    df_subs_in = df_events.filter(col("type.name") == "Substitution") \
        .select(
            col("match_id"), 
            col("substitution.replacement.id").cast(StringType()).alias("player_statsbomb_id"),
            col("minute").alias("entry_min")
        )
    
    df_entries = df_starters.unionByName(df_subs_in)

    # C. Xác định thời điểm RỜI SÂN
    df_subs_out = df_events.filter(col("type.name") == "Substitution") \
        .select(
            col("match_id"), 
            col("player.id").cast(StringType()).alias("player_statsbomb_id"),
            col("minute").alias("exit_min")
        )

    df_red_cards = df_events.filter(
        col("bad_behaviour.card.name").isin("Red Card", "Second Yellow") | 
        col("foul_committed.card.name").isin("Red Card", "Second Yellow")
    ).select(
        col("match_id"), 
        col("player.id").cast(StringType()).alias("player_statsbomb_id"),
        col("minute").alias("exit_min")
    )

    df_exits = df_subs_out.unionByName(df_red_cards)

    # D. TÍNH TOÁN CUỐI CÙNG (Minutes)
    df_mins_calc = df_entries.join(df_exits, ["match_id", "player_statsbomb_id"], "left") \
        .join(df_match_duration, "match_id", "inner") \
        .select(
            col("match_id"),
            col("player_statsbomb_id"),
            (coalesce(col("exit_min"), col("match_end_min")) - col("entry_min")).alias("minutes_played_calc")
        )

    # ==========================================
    # 3. AGGREGATION CÁC CHỈ SỐ (UPDATE LOGIC MỚI)
    # ==========================================
    
    df_metrics = df_events.select(
        col("match_id"),
        col("team.id").cast(StringType()).alias("team_natural_id"),
        col("player.id").cast(StringType()).alias("player_statsbomb_id"),
        col("type.name").alias("type_name"),
        col("location"), # Cần location để tính TiB
        
        # Check cột tồn tại an toàn
        (col("pass.outcome.name") if "pass" in df_events.columns else lit(None)).alias("pass_outcome"),
        (col("pass.goal_assist") if "pass" in df_events.columns else lit(None)).alias("is_assist"),
        (col("shot.outcome.name") if "shot" in df_events.columns else lit(None)).alias("shot_outcome"),
        (col("shot.statsbomb_xg") if "shot" in df_events.columns else lit(0)).alias("xg"),
        (col("duel.type.name") if "duel" in df_events.columns else lit(None)).alias("duel_type"),
        (col("duel.outcome.name") if "duel" in df_events.columns else lit(None)).alias("duel_outcome")
    )

    df_player_events = df_metrics.filter(col("player_statsbomb_id").isNotNull())

    # --- LOGIC NÂNG CAO ---
    
    # 1. Xác định vùng cấm địa (Attacking Box): x >= 102, 18 <= y <= 62
    is_in_box = (col("location").getItem(0) >= 102) & \
                (col("location").getItem(1) >= 18) & \
                (col("location").getItem(1) <= 62)

    # 2. Danh sách Outcome tính là Tackle THẮNG
    tackle_won_outcomes = ["Success In Play", "Won", "Success Out"]

    # Tính toán Stats
    df_stats_agg = df_player_events.groupBy("match_id", "team_natural_id", "player_statsbomb_id") \
        .agg(
            # Các chỉ số cơ bản
            sum(when(col("type_name") == "Pass", 1).otherwise(0)).alias("total_passes"),
            sum(when((col("type_name") == "Pass") & (col("pass_outcome").isNull()), 1).otherwise(0)).alias("accurate_passes"),
            sum(when(col("type_name") == "Shot", 1).otherwise(0)).alias("total_shots"),
            sum(when((col("type_name") == "Shot") & (col("shot_outcome") == "Goal"), 1).otherwise(0)).alias("goals"),
            sum(when(col("is_assist") == True, 1).otherwise(0)).alias("assists"),
            sum(coalesce(col("xg"), lit(0))).cast("numeric(6,3)").alias("xg_total"),
            lit(0).cast("numeric(6,3)").alias("xa_total"),
            sum(when(col("type_name") == "Interception", 1).otherwise(0)).alias("interceptions"),
            
            # Các chỉ số nâng cao
            # TiB
            sum(when(is_in_box, 1).otherwise(0)).alias("touches_in_box"),
            # TSR (Total)
            sum(when((col("type_name") == "Duel") & (col("duel_type") == "Tackle"), 1).otherwise(0)).alias("tackles_total"),
            # TSR (Won)
            sum(when(
                (col("type_name") == "Duel") & 
                (col("duel_type") == "Tackle") & 
                (col("duel_outcome").isin(tackle_won_outcomes)), 1
            ).otherwise(0)).alias("tackles_won")
        )

    # Join Stats với Minutes Played
    df_stats_full = df_stats_agg.join(
        df_mins_calc, 
        ["match_id", "player_statsbomb_id"], 
        "inner"
    ).withColumnRenamed("minutes_played_calc", "minutes_played")

    # ==========================================
    # 4. LOOKUP SK & WRITE
    # ==========================================
    print(">>> Lookup & Write...")

    dim_match = spark.read.jdbc(POSTGRES_URL, "dim_match", properties=POSTGRES_PROPS) \
        .select("match_sk", "match_statsbomb_id", "match_date")

    dim_team = spark.read.jdbc(POSTGRES_URL, "dim_team", properties=POSTGRES_PROPS) \
        .select("team_sk", "team_natural_id", "effective_from", "effective_to")

    dim_player = spark.read.jdbc(POSTGRES_URL, "dim_player", properties=POSTGRES_PROPS) \
        .select("player_sk", "player_statsbomb_id", "effective_from", "effective_to")
        
    dim_date = spark.read.jdbc(POSTGRES_URL, "dim_date", properties=POSTGRES_PROPS) \
        .select("date_id", "date_value")

    # Join Chain
    df_j1 = df_stats_full.join(dim_match, df_stats_full.match_id == dim_match.match_statsbomb_id, "inner")
    df_j2 = df_j1.join(dim_date, df_j1.match_date == dim_date.date_value, "left")
    
    cond_team = (df_j2.team_natural_id == dim_team.team_natural_id) & \
                (df_j2.match_date >= dim_team.effective_from) & \
                (df_j2.match_date <= dim_team.effective_to)
    df_j3 = df_j2.join(dim_team, cond_team, "left")

    cond_player = (df_j3.player_statsbomb_id == dim_player.player_statsbomb_id) & \
                  (df_j3.match_date >= dim_player.effective_from) & \
                  (df_j3.match_date <= dim_player.effective_to)
    df_final_join = df_j3.join(dim_player, cond_player, "left")

    # Final Select (Lưu ý ánh xạ đúng tên cột mới)
    df_write = df_final_join.select(
        col("date_id"),
        dim_player["player_sk"],
        dim_match["match_sk"],
        dim_team["team_sk"],
        col("minutes_played"),
        col("total_passes"),
        col("accurate_passes"),
        col("total_shots"),
        col("goals"),
        col("assists"),
        col("xg_total"),
        col("xa_total"),
        col("interceptions"),
        # Cột mới
        col("touches_in_box"),
        col("tackles_total"),
        col("tackles_won"),
        
        current_timestamp().alias("created_at")
    ).filter(
        col("player_sk").isNotNull() & 
        col("team_sk").isNotNull() & 
        col("date_id").isNotNull()
    )

    print(f">>> [FACT_STATS] Writing {df_write.count()} rows to Postgres...")

    try:
        df_write.write.jdbc(
            url=POSTGRES_URL, 
            table="fact_player_match_stats", 
            mode="append", 
            properties=POSTGRES_PROPS
        )
        print(">>> Success!")
    except Exception as e:
        print(f"!!! ERROR: {str(e)}")

    spark.stop()

if __name__ == "__main__":
    run_etl()