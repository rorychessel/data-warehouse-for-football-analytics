import psycopg2
import requests
import time
import sys

# --- CONFIG ---
DB_HOST = "warehouse_db"
DB_NAME = "football_dw"
DB_USER = "admin"
DB_PASS = "root"
DB_PORT = "5432"

# Session dùng chung để tối ưu kết nối
session = requests.Session()
session.headers.update({'User-Agent': 'FootballDataWarehouse/2.0 (student_project)'})

def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS, port=DB_PORT
        )
        return conn
    except Exception as e:
        print(f"Error connecting to DB: {e}")
        sys.exit(1)

def search_wikidata_id(name):
    """
    Dùng Wikipedia Search API để tìm Wikidata ID (QID) từ tên người.
    API này tự động xử lý mờ (fuzzy match), bỏ dấu, tên đệm...
    """
    url = "https://en.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "list": "search",
        "srsearch": name,
        "format": "json",
        "srlimit": 1  # Chỉ lấy kết quả đầu tiên (thường là đúng nhất)
    }
    try:
        r = session.get(url, params=params, timeout=5)
        data = r.json()
        if data.get("query", {}).get("search"):
            title = data["query"]["search"][0]["title"]
            
            # Sau khi có Title bài viết (VD: Jordi Alba), gọi tiếp để lấy QID
            prop_params = {
                "action": "query",
                "prop": "pageprops",
                "titles": title,
                "ppprop": "wikibase_item",
                "format": "json"
            }
            r2 = session.get(url, params=prop_params, timeout=5)
            d2 = r2.json()
            pages = d2.get("query", {}).get("pages", {})
            for k, v in pages.items():
                if "pageprops" in v and "wikibase_item" in v["pageprops"]:
                    return v["pageprops"]["wikibase_item"] # Trả về QID (ví dụ Q12345)
    except:
        pass
    return None

def get_birth_year_from_qid(qid):
    """
    Dùng Wikidata API để lấy năm sinh từ QID
    """
    url = f"https://www.wikidata.org/w/api.php"
    params = {
        "action": "wbgetclaims",
        "entity": qid,
        "property": "P569", # P569 là Date of Birth
        "format": "json"
    }
    try:
        r = session.get(url, params=params, timeout=5)
        data = r.json()
        if "claims" in data and "P569" in data["claims"]:
            # Lấy giá trị ngày sinh: +1989-03-21T00:00:00Z
            date_str = data["claims"]["P569"][0]["mainsnak"]["datavalue"]["value"]["time"]
            # Xử lý chuỗi để lấy năm (bỏ dấu + ở đầu nếu có)
            year = int(date_str.replace("+", "").split("-")[0])
            return year
    except:
        pass
    return None

def clean_name_fallback(full_name):
    """
    Chiến thuật phụ: Rút gọn tên.
    VD: "Lionel Andrés Messi Cuccittini" -> "Lionel Messi"
    """
    parts = full_name.split()
    if len(parts) > 2:
        # Lấy từ đầu và từ cuối
        return f"{parts[0]} {parts[-1]}"
    return None

def run_enrichment():
    print(">>> BẮT ĐẦU TỰ ĐỘNG BỔ SUNG NĂM SINH (VERSION 2 - SMART SEARCH)...")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Lấy danh sách chưa có năm sinh
    cur.execute("SELECT player_sk, player_name FROM dim_player WHERE birth_year IS NULL ORDER BY player_sk")
    players = cur.fetchall()
    
    total = len(players)
    print(f">>> Tìm thấy {total} cầu thủ cần xử lý.")
    
    updated_count = 0
    
    for index, (sk, name) in enumerate(players):
        print(f"[{index+1}/{total}] {name}...", end=" ", flush=True)
        
        # Chiến thuật 1: Tìm bằng tên gốc (Full name)
        qid = search_wikidata_id(name)
        
        # Chiến thuật 2: Nếu không thấy, thử rút gọn tên (Fallback)
        if not qid:
            short_name = clean_name_fallback(name)
            if short_name:
                # print(f"(try: {short_name})", end=" ")
                qid = search_wikidata_id(short_name)
        
        year = None
        if qid:
            year = get_birth_year_from_qid(qid)
            
        if year:
            # Kiểm tra năm sinh hợp lý (1960 - 2010) để tránh trash data
            if 1960 <= year <= 2010:
                cur.execute(
                    "UPDATE dim_player SET birth_year = %s WHERE player_sk = %s",
                    (year, sk)
                )
                conn.commit()
                print(f"-> OK ({year})")
                updated_count += 1
            else:
                print(f"-> Skip (Year {year} invalid)")
        else:
            print("-> NOT FOUND")
            
        # Sleep nhẹ để tôn trọng API
        time.sleep(0.3)

    print("-" * 30)
    print(f"HOÀN THÀNH V2. Đã cập nhật thêm: {updated_count}")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    run_enrichment()