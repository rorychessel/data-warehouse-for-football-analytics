from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# --- CONFIG ---
MINIO_CONF = {
    "endpoint": "http://minio:9000",
    "access_key": "minio_admin",
    "secret_key": "minio_password"
}

def run_debug():
    spark = SparkSession.builder \
        .appName("DEBUG_Dim_Player") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_CONF["endpoint"]) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_CONF["access_key"]) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_CONF["secret_key"]) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()
    
    print("----------------------------------------------------------------")
    print(">>> BẮT ĐẦU DEBUG...")

    # 1. Kiểm tra Matches
    try:
        df_matches = spark.read.json("s3a://football-data/raw/matches/*.json")
        count_m = df_matches.count()
        print(f">>> 1. Số lượng trận đấu (Matches): {count_m}")
        if count_m > 0:
            df_matches.select("match_id").printSchema()
            df_matches.select("match_id").show(3)
    except Exception as e:
        print(f"!!! LỖI đọc Matches: {e}")

    # 2. Kiểm tra Lineups
    try:
        df_lineups = spark.read.json("s3a://football-data/raw/lineups/*/*.json")
        count_l = df_lineups.count()
        print(f">>> 2. Số lượng file Lineups tìm thấy: {count_l}")
        
        if count_l == 0:
            print("!!! CẢNH BÁO: Không tìm thấy dữ liệu Lineups. Hãy kiểm tra lại MinIO!")
            spark.stop()
            return

        df_lineups.select("match_id", "team_id").printSchema()
        df_lineups.select("match_id").show(3)
        
    except Exception as e:
        print(f"!!! LỖI đọc Lineups: {e}")
        spark.stop()
        return

    # 3. Kiểm tra Explode
    try:
        df_exploded = df_lineups.select(
            col("match_id"),
            explode("lineup").alias("p")
        )
        count_e = df_exploded.count()
        print(f">>> 3. Số lượng cầu thủ sau khi Explode: {count_e}")
    except Exception as e:
        print(f"!!! LỖI khi Explode: {e}")

    # 4. Kiểm tra Join
    try:
        # Ép kiểu match_id về cùng loại (Integer) để chắc chắn join được
        df_m_clean = df_matches.select(col("match_id").cast("long"), "match_date")
        df_l_clean = df_exploded.select(col("match_id").cast("long"), "p")
        
        df_joined = df_l_clean.join(df_m_clean, on="match_id", how="inner")
        count_j = df_joined.count()
        print(f">>> 4. Số lượng dòng sau khi JOIN: {count_j}")
        
        if count_j == 0 and count_e > 0:
            print("!!! LÝ DO: Có dữ liệu nhưng JOIN bị trượt hết. Kiểm tra kiểu dữ liệu match_id.")
            
    except Exception as e:
        print(f"!!! LỖI khi Join: {e}")

    print("----------------------------------------------------------------")
    spark.stop()

if __name__ == "__main__":
    run_debug()