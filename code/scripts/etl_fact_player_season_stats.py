from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# --- CONFIG ---
POSTGRES_PROPS = { "user": "admin", "password": "root", "driver": "org.postgresql.Driver" }
POSTGRES_URL = "jdbc:postgresql://warehouse_db:5432/football_dw"

def get_spark_session():
    return SparkSession.builder \
        .appName("ETL_Fact_Season_Stats_Optimized") \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar") \
        .getOrCreate()

def run_etl():
    spark = get_spark_session()
    print(">>> [FACT_SEASON_STATS] Bắt đầu tổng hợp từ Fact Match Stats...")

    # ==========================================
    # 1. READ DATA FROM POSTGRES (SILVER LAYER)
    # ==========================================
    print(">>> Reading source tables from DB...")
    
    # Đọc bảng Fact Match
    df_match_stats = spark.read.jdbc(POSTGRES_URL, "fact_player_match_stats", properties=POSTGRES_PROPS) \
        .select(
            "player_sk", 
            "team_sk", 
            "match_sk", 
            "minutes_played", 
            "goals", 
            "assists", 
            "xg_total", 
            "xa_total", 
            "interceptions",
            "touches_in_box",
            "tackles_total",
            "tackles_won"
        )

    # Đọc bảng Dim Match để lấy thông tin Mùa giải (Season)
    df_dim_match = spark.read.jdbc(POSTGRES_URL, "dim_match", properties=POSTGRES_PROPS) \
        .select("match_sk", "season")

    # ==========================================
    # 2. JOIN & AGGREGATE (GOLD LAYER LOGIC)
    # ==========================================
    print(">>> Aggregating...")

    # Join để gắn Season vào từng dòng stats
    df_joined = df_match_stats.join(df_dim_match, on="match_sk", how="inner")

    # Group By: Season + Player + Team
    # Logic: Sum tất cả các chỉ số
    df_season_stats = df_joined.groupBy("season", "player_sk", "team_sk") \
        .agg(
            # Số trận ra sân
            count("match_sk").alias("matches_played"),
            
            # Tổng hợp các chỉ số cơ bản
            sum("goals").alias("goals"),
            sum("assists").alias("assists"),
            sum("minutes_played").alias("minutes"),
            sum("xg_total").cast("numeric(7,3)").alias("xg_season"),
            sum("xa_total").cast("numeric(7,3)").alias("xa_season"),
            sum("interceptions").alias("interceptions"),
            
            # Tổng hợp các chỉ số nâng cao (Mới)
            sum("touches_in_box").alias("touches_in_box"),
            sum("tackles_total").alias("tackles_total"),
            sum("tackles_won").alias("tackles_won"),

            # Placeholder cho các cột chưa có dữ liệu gốc
            lit(0).alias("starts"),
            lit(0).alias("yellow_cards"),
            lit(0).alias("red_cards")
        )

    # ==========================================
    # 3. WRITE TO POSTGRES
    # ==========================================
    
    df_write = df_season_stats.select(
        col("player_sk"),
        col("team_sk"),
        col("season"),
        col("matches_played"),
        col("starts"),
        col("goals"),
        col("assists"),
        col("minutes"),
        col("yellow_cards"),
        col("red_cards"),
        col("xg_season"),
        col("xa_season"),
        col("interceptions"),
        
        # Write cột mới
        col("touches_in_box"),
        col("tackles_total"),
        col("tackles_won"),
        
        current_timestamp().alias("created_at")
    )

    print(f">>> [FACT_SEASON_STATS] Writing {df_write.count()} rows to Postgres...")

    try:
        # Lưu ý: Nếu bảng chưa có các cột mới, cần chạy lệnh SQL ALTER TABLE trước
        df_write.write.jdbc(
            url=POSTGRES_URL, 
            table="fact_player_season_stats", 
            mode="append", 
            properties=POSTGRES_PROPS
        )
        print(">>> Success!")
    except Exception as e:
        print(f"!!! ERROR: {str(e)}")

    spark.stop()

if __name__ == "__main__":
    run_etl()