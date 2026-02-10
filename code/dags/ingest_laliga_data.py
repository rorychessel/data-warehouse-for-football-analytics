from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import requests
import json
import logging

# ======================================================
# PHẦN 1: CẤU HÌNH & HÀM INGEST (INCREMENTAL LOAD)
# ======================================================

BASE_URL = "https://raw.githubusercontent.com/statsbomb/open-data/master/data"
BUCKET_NAME = "football-data"
CONNECTION_ID = "minio_conn"
SCRIPT_DIR = "/opt/airflow/scripts"
POSTGRES_JAR = "/opt/airflow/jars/postgresql-42.6.0.jar"

# Các mùa giải cần lấy (La Liga)
TARGET_SEASONS = ['2017/2018', '2018/2019', '2019/2020']
TARGET_COMPETITION_NAME = "La Liga"

def upload_json_to_minio(data, key):
    """Hàm upload JSON lên MinIO"""
    try:
        hook = S3Hook(aws_conn_id=CONNECTION_ID)
        if not hook.check_for_bucket(BUCKET_NAME):
            hook.create_bucket(BUCKET_NAME)
            
        hook.load_string(
            string_data=json.dumps(data, ensure_ascii=False),
            key=key,
            bucket_name=BUCKET_NAME,
            replace=True
        )
    except Exception as e:
        logging.error(f"FAIL upload {key}: {str(e)}")
        raise e

def fetch_and_save_file(url, minio_key, description):
    """Hàm tải file có kiểm tra tồn tại để tránh tải lại không cần thiết"""
    hook = S3Hook(aws_conn_id=CONNECTION_ID)
    
    # Nếu file đã có -> Bỏ qua
    if hook.check_for_key(key=minio_key, bucket_name=BUCKET_NAME):
        logging.info(f"SKIP (Exist): {description}")
        return True
    
    try:
        resp = requests.get(url)
        if resp.status_code == 200:
            upload_json_to_minio(resp.json(), minio_key)
            logging.info(f"DOWNLOADED: {description} -> {minio_key}")
            return True
        elif resp.status_code == 404:
            logging.warning(f"404 Not Found - {description}")
            return False
        else:
            logging.error(f"Error {resp.status_code} - {description}")
            return False
    except Exception as e:
        logging.error(f"Exception - {description}: {str(e)}")
        return False

def extract_and_load_laliga(**kwargs):
    """
    Hàm chính: Tải dữ liệu theo NGÀY (Incremental)
    - Nhận ngày chạy từ Airflow (ds)
    - Lọc trận đấu diễn ra trong ngày đó
    - Lưu vào folder partition: date=YYYY-MM-DD
    """
    # Lấy ngày thực thi từ Airflow (ví dụ: '2018-05-06')
    target_date = kwargs.get('ds')
    logging.info(f"=== BẮT ĐẦU INGEST CHO NGÀY: {target_date} ===")

    # 1. Tải Metadata Competitions (để lấy season_id)
    comp_url = f"{BASE_URL}/competitions.json"
    resp = requests.get(comp_url)
    competitions = resp.json()
    
    # Lọc ra các cấu hình La Liga cần thiết
    target_configs = []
    for comp in competitions:
        if (comp['competition_name'] == TARGET_COMPETITION_NAME and 
            comp['season_name'] in TARGET_SEASONS):
            target_configs.append(comp)
            
    processed_count = 0

    # 2. Duyệt từng mùa giải để tìm trận đấu
    for config in target_configs:
        comp_id = config['competition_id']
        season_id = config['season_id']
        
        # Tải danh sách trận đấu của mùa này
        match_url = f"{BASE_URL}/matches/{comp_id}/{season_id}.json"
        matches_resp = requests.get(match_url)
        if matches_resp.status_code != 200: continue
        
        matches_data = matches_resp.json()
        
        # --- LOGIC LỌC: CHỈ LẤY TRẬN ĐẤU CỦA NGÀY target_date ---
        daily_matches = [m for m in matches_data if m.get('match_date') == target_date]
        
        if not daily_matches:
            continue

        logging.info(f"Mùa {config['season_name']}: Tìm thấy {len(daily_matches)} trận ngày {target_date}")

        for match in daily_matches:
            match_id = match['match_id']
            processed_count += 1
            
            # --- QUAN TRỌNG: LƯU THEO FOLDER 'date=YYYY-MM-DD' ---
            # Điều này giúp Spark tìm file cực nhanh mà không cần quét toàn bộ
            
            # 1. Tải Lineups
            fetch_and_save_file(
                url=f"{BASE_URL}/lineups/{match_id}.json",
                minio_key=f"raw/lineups/date={target_date}/{match_id}.json", 
                description=f"Lineup {match_id}"
            )

            # 2. Tải Events
            fetch_and_save_file(
                url=f"{BASE_URL}/events/{match_id}.json",
                minio_key=f"raw/events/date={target_date}/{match_id}.json", 
                description=f"Event {match_id}"
            )
            
    if processed_count == 0:
        logging.info(f"Không có trận đấu nào trong ngày {target_date}.")
    else:
        logging.info(f"Hoàn tất ingest {processed_count} trận đấu.")

# ======================================================
# PHẦN 2: ĐỊNH NGHĨA DAG
# ======================================================

default_args = {
    'owner': 'nguyen_phu_vinh',
    'depends_on_past': False,
    # Đặt ngày bắt đầu xa về quá khứ để có thể Backfill dữ liệu lịch sử
    'start_date': datetime(2017, 8, 1), 
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'football_dw_pipeline_incremental', 
    default_args=default_args,
    description='ETL La Liga: Incremental Load by Date Partition',
    schedule_interval='@daily', # Chạy hàng ngày
    catchup=False, # Tắt catchup tự động để tránh treo máy, ta sẽ chạy Backfill thủ công
    tags=['football', 'etl', 'incremental']
) as dag:

    # --- TASK 1: INGEST ---
    ingest_task = PythonOperator(
        task_id='ingest_daily_data',
        python_callable=extract_and_load_laliga,
        provide_context=True, # Bắt buộc True để nhận biến {{ ds }}
        execution_timeout=timedelta(minutes=30)
    )

    # --- HELPER: TẠO SPARK TASK ---
    def create_spark_task(task_id, file_name):
        return SparkSubmitOperator(
            task_id=task_id,
            application=f"{SCRIPT_DIR}/{file_name}",
            conn_id='spark_default',
            jars=POSTGRES_JAR,
            verbose=True,
            # --- TRUYỀN NGÀY VÀO SPARK QUA ARGUMENTS ---
            application_args=[
                "--process_date", "{{ ds }}" 
            ],
            conf={
                "spark.executor.memory": "512m", 
                "spark.driver.memory": "512m"
            }
        )

    # --- TASK 2: LEVEL 1 - BASE DIMENSIONS ---
    t_dim_date = create_spark_task("dim_date", "etl_dim_date.py")
    t_dim_loc = create_spark_task("dim_location", "etl_dim_location.py")
    t_dim_type = create_spark_task("dim_event_type", "etl_dim_event_type.py")
    t_dim_pattern = create_spark_task("dim_play_pattern", "etl_dim_play_pattern.py")
    t_dim_team = create_spark_task("dim_team", "etl_dim_team.py")
    t_dim_player = create_spark_task("dim_player", "etl_dim_player.py")

    # --- TASK 3: LEVEL 2 - DEPENDENT DIMENSIONS ---
    t_dim_match = create_spark_task("dim_match", "etl_dim_match.py")

    # --- TASK 4: LEVEL 3 - FACT TABLES ---
    t_fact_event = create_spark_task("fact_event", "etl_fact_event.py")

    # --- TASK 5: LEVEL 4 - AGGREGATE STATS ---
    t_fact_player_match = create_spark_task("fact_player_match_stats", "etl_fact_player_match_stats.py")
    t_fact_team_match = create_spark_task("fact_team_match_stats", "etl_fact_team_match_stats.py")
    t_fact_player_season = create_spark_task("fact_player_season_stats", "etl_fact_player_season_stats.py")

    # --- DEPENDENCIES (LUỒNG CHẠY) ---
    ingest_task >> [t_dim_date, t_dim_loc, t_dim_type, t_dim_pattern, t_dim_team, t_dim_player]
    
    t_dim_team >> t_dim_match
    
    [t_dim_date, t_dim_loc, t_dim_type, t_dim_pattern, t_dim_player, t_dim_match] >> t_fact_event
    
    t_fact_event >> [t_fact_player_match, t_fact_team_match]
    
    t_fact_player_match >> t_fact_player_season