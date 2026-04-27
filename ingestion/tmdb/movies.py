import os
import sys
import requests
import json
from dotenv import load_dotenv
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from ingestion.common.upload_data import upload_to_minio
from ingestion.common.redis_utils import is_movie_changed, save_movie_state

load_dotenv()
TMDB_API_KEY = os.getenv("TMDB_API_KEY")
BASE_URL = "https://api.themoviedb.org/3"

def fetch_movie_genres():
    url = f"{BASE_URL}/genre/movie/list"
    params = {"api_key": TMDB_API_KEY, "language": "en-US"}
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            genres = response.json().get("genres", [])
            return {g["id"]: g["name"] for g in genres}
    except Exception as e:
        print(f"Cảnh báo: Không lấy được genre mapping: {e}")
    return {}

GENRE_MAP = fetch_movie_genres()

def fetch_trending_movie_ids(limit=100):
    page = 1
    all_movie_ids = []
    while len(all_movie_ids) < limit:
        url = f"{BASE_URL}/trending/movie/day"
        params = {
            "api_key": TMDB_API_KEY,
            "page": page
            }
        try:
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                results = data.get("results", [])
                for movie in results:
                    if movie["id"] not in all_movie_ids:
                        all_movie_ids.append(movie["id"])
                    if len(all_movie_ids) >= limit: 
                        break
            if page >= data.get("total_pages", 0):
                break
            page += 1
        except Exception as e:
            print(f"Lỗi lấy danh sách Trending: {e}")
            return []
    return all_movie_ids

def fetch_movie_details(movie_id):
    url = f"{BASE_URL}/movie/{movie_id}"
    params = {"api_key": TMDB_API_KEY, "language": "en-US"}
    try:
        response = requests.get(url, params=params)
        if response.status_code != 200:
            return f"Metadata TMDB Error {movie_id}: Status {response.status_code}"
        movie_data = response.json()
        
        if "genres" in movie_data and isinstance(movie_data["genres"], list):
            movie_data["genres"] = [g["name"] for g in movie_data["genres"] if "name" in g]

    except Exception as e:
        return f"Metadata TMDB Error {movie_id}: {str(e)}"
    return movie_data

def ingest_tmdb_movie(movie_id):
    movie_data = fetch_movie_details(movie_id)
    # ===== REDIS DEDUP: So sánh hash với lần cào trước =====
    if not is_movie_changed("tmdb", str(movie_id), movie_data):
        # Dữ liệu giống hệt → chỉ refresh TTL, không upload lại
        save_movie_state("tmdb", str(movie_id), movie_data)
        return f"Metadata TMDB SKIP (không đổi): {movie_id}"

    # Dữ liệu mới hoặc đã thay đổi → upload lên MinIO
    upload_to_minio(
        raw_data=[movie_data],
        source="tmdb",
        entity="movies",
        methods="Direct API Call (Requests)",
        http_status=200,
        search_params={"movie_id": movie_id}
    )
    save_movie_state("tmdb", str(movie_id), movie_data)
    return f"Metadata TMDB OK (mới/cập nhật): {movie_id}"

# if __name__ == "__main__":
#     movie_details = fetch_movie_details("687163")
#     movie_details_json = json.dumps(movie_details, indent=4)
#     print(movie_details_json)

