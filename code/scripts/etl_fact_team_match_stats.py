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
        .appName("ETL_Fact_Team_Stats_Optimized") \
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
    print(">>> [FACT_TEAM_STATS] Bắt đầu xử lý (Phiên bản Tối ưu)...")

    # ==========================================
    # 1. READ DIMENSIONS (Lookup Table)
    # ==========================================
    print(">>> Reading Dimensions...")
    
    # Đọc Dim Match: Lấy thông tin Tỷ số và SK
    dim_match = spark.read.jdbc(POSTGRES_URL, "dim_match", properties=POSTGRES_PROPS) \
        .select(
            "match_sk", "match_statsbomb_id", "match_date", 
            "home_team_sk", "away_team_sk", "home_score", "away_score"
        )

    # Đọc Dim Team: Để map từ Natural ID (trong Event) sang Team SK
    dim_team = spark.read.jdbc(POSTGRES_URL, "dim_team", properties=POSTGRES_PROPS) \
        .select("team_sk", "team_natural_id", "effective_from", "effective_to")

    # ==========================================
    # 2. READ RAW EVENTS & AGGREGATE
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

    # Chỉ chọn các cột cần thiết để tính xG, Possession, PPDA
    def_actions = ["Pressure", "Duel", "Interception", "Block", "Foul Committed"]
    
    df_metrics = df_events.select(
        col("match_id"),
        col("team.id").cast(StringType()).alias("team_natural_id"),
        col("type.name").alias("type_name"),
        col("duration"),
        (col("shot.statsbomb_xg") if "shot" in df_events.columns else lit(0)).alias("xg")
    ).filter(col("team_natural_id").isNotNull())

    # --- AGGREGATION CƠ BẢN ---
    df_agg_basic = df_metrics.groupBy("match_id", "team_natural_id") \
        .agg(
            # Tổng xG
            sum(coalesce(col("xg"), lit(0))).cast("numeric(6,3)").alias("total_xg"),
            
            # Thời gian cầm bóng (để tính %) - Clean overflow duration > 999
            sum(when(abs(col("duration")) > 999, 0).otherwise(coalesce(col("duration"), lit(0)))).alias("my_duration"),
            
            # Số đường chuyền của MÌNH (để tính PPDA cho đối thủ)
            sum(when(col("type_name") == "Pass", 1).otherwise(0)).alias("my_pass_count"),
            
            # Số hành động phòng ngự của MÌNH (để tính PPDA cho mình)
            sum(when(col("type_name").isin(def_actions), 1).otherwise(0)).alias("my_def_action_count")
        )

    # ==========================================
    # 3. WINDOW FUNCTION (Thay thế Self-Join)
    # ==========================================
    # Tính tổng Duration và Pass của CẢ TRẬN (Match Total) ngay trên cùng DataFrame
    
    w_match = Window.partitionBy("match_id")
    
    df_calc = df_agg_basic \
        .withColumn("match_total_duration", sum("my_duration").over(w_match)) \
        .withColumn("match_total_passes", sum("my_pass_count").over(w_match)) \
        .withColumn("opponent_pass_count", col("match_total_passes") - col("my_pass_count"))

    # ==========================================
    # 4. JOIN & LOGIC
    # ==========================================
    print(">>> Joining & Applying Logic...")

    # 4.1 Join Dim Team để lấy SK của đội mình
    # Join Event với Dim Match trước để có Date, sau đó mới Join Team.
    
    # A. Join Event AGG với Dim Match
    df_j_match = df_calc.join(
        dim_match, 
        df_calc.match_id == dim_match.match_statsbomb_id, 
        "inner"
    )

    # B. Tính Date ID (Tối ưu theo ý bạn: YYYYMMDD từ chuỗi)
    df_w_date = df_j_match.withColumn(
        "date_id", 
        date_format(col("match_date"), "yyyyMMdd").cast(IntegerType())
    )

    # C. Join Dim Team (Lấy My Team SK)
    cond_team = (
        (df_w_date.team_natural_id == dim_team.team_natural_id) &
        (df_w_date.match_date >= dim_team.effective_from) &
        (df_w_date.match_date <= dim_team.effective_to)
    )
    df_j_team = df_w_date.join(dim_team, cond_team, "left")

    # ==========================================
    # 5. FINAL LOGIC (Opponent & Result from Dim_Match)
    # ==========================================
    # Logic: Dựa vào SK của đội mình so với Home/Away SK trong Dim Match
    
    df_final = df_j_team.withColumn(
        "is_home_team", 
        when(col("team_sk") == col("home_team_sk"), True).otherwise(False)
    ).select(
        col("date_id"),
        col("team_sk"),
        col("match_sk"),
        
        # Xác định Opponent SK dựa trên mình là Home hay Away
        when(col("is_home_team"), col("away_team_sk"))
            .otherwise(col("home_team_sk")).alias("opponent_team_sk"),
            
        # Xác định Goals Scored / Conceded từ Dim Match (Source of Truth)
        when(col("is_home_team"), col("home_score"))
            .otherwise(col("away_score")).alias("goals_scored"),
            
        when(col("is_home_team"), col("away_score"))
            .otherwise(col("home_score")).alias("goals_conceded"),

        # Tính toán Metrics
        col("total_xg"),
        
        # Possession % = My Duration / Total Duration * 100
        when(col("match_total_duration") > 0, 
             (col("my_duration") / col("match_total_duration") * 100))
        .otherwise(50).cast("numeric(5,2)").alias("possession_pct"),
        
        # PPDA = Opponent Passes / My Def Actions
        when(col("my_def_action_count") > 0,
             col("opponent_pass_count") / col("my_def_action_count"))
        .otherwise(None).cast("numeric(6,2)").alias("ppda")
    )

    # Thêm cột Result và Created At
    df_write = df_final.withColumn(
        "result",
        when(col("goals_scored") > col("goals_conceded"), "W")
        .when(col("goals_scored") < col("goals_conceded"), "L")
        .otherwise("D")
    ).withColumn("created_at", current_timestamp()) \
     .filter(col("team_sk").isNotNull() & col("opponent_team_sk").isNotNull())

    print(f">>> [FACT_TEAM_STATS] Writing {df_write.count()} rows to Postgres...")

    try:
        df_write.write.jdbc(
            url=POSTGRES_URL, 
            table="fact_team_match_stats", 
            mode="append", 
            properties=POSTGRES_PROPS
        )
        print(">>> Success!")
    except Exception as e:
        print(f"!!! ERROR: {str(e)}")

    spark.stop()

if __name__ == "__main__":
    run_etl()