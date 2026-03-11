import os
import sys
import requests
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Thêm đường dẫn để import từ thư mục cha (ingestion/)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from upload_data import upload_to_minio

load_dotenv()

TMDB_TOKEN = os.getenv("TMDB_READ_ACCESS_TOKEN")
HEADERS = {
    "accept": "application/json",
    "Authorization": f"Bearer {TMDB_TOKEN}"
}

def fetch_reviews_for_movie(movie_id, movie_title, max_pages=1):
    """
    Lấy danh sách bình luận của một bộ phim cụ thể.
    """
    reviews_buffer = []
    for page in range(1, max_pages + 1):
        url = f"https://api.themoviedb.org/3/movie/{movie_id}/reviews"
        try:
            response = requests.get(url, headers=HEADERS, params={"page": page})
            if response.status_code == 200:
                data = response.json()
                results = data.get("results", [])
                if not results:
                    break
                
                for r in results:
                    # Bổ sung ID phim vào bản ghi review để dễ link dữ liệu sau này
                    r["movie_id"] = movie_id
                    r["movie_title"] = movie_title
                    reviews_buffer.append(r)
                
                if page >= data.get("total_pages"):
                    break
                
                time.sleep(0.1) # Tránh bị rate limit
            else:
                print(f"⚠️ Không thể lấy review phim {movie_title} (Status: {response.status_code})")
                break
        except Exception as e:
            print(f"Lỗi khi gọi API Reviews cho phim {movie_id}: {e}")
            break
            
    return reviews_buffer

def ingest_tmdb_data(movie_pages=1, reviews_per_movie_pages=1):
    """
    Luồng chính: Lấy danh sách phim mới -> Lấy Reviews -> Đẩy lên MinIO.
    """
    discover_url = "https://api.themoviedb.org/3/discover/movie"
    today = datetime.now()
    last_month = today - timedelta(days=30)
    
    print(f"🚀 Bắt đầu thu thập dữ liệu TMDB từ ngày: {last_month.strftime('%Y-%m-%d')}")

    for page in range(1, movie_pages + 1):
        params = {
            "language": "en-US",
            "sort_by": "popularity.desc",
            "primary_release_date.gte": last_month.strftime('%Y-%m-%d'),
            "page": page
        }
        try:
            print(f"\n--- Đang xử lý Trang {page}/{movie_pages} ---")
            response = requests.get(discover_url, headers=HEADERS, params=params)
            
            if response.status_code != 200:
                print(f"❌ Lỗi API Discover (Status: {response.status_code})")
                continue
                
            movies = response.json().get("results", [])
            if not movies:
                print("Không tìm thấy phim nào ở trang này.")
                continue

            # 1. Đẩy danh sách phim của trang này lên Bronze
            upload_to_minio(
                data_list=movies,
                source="tmdb",
                entity="movies",
                extraction_method="API Calling",
                http_status=200,
                search_params={"page": page, "sort_by": "popularity.desc"}
            )

            # 2. Thu thập và đẩy Reviews của các phim vừa lấy được
            print(f"🔍 Đang gom bình luận cho {len(movies)} phim...")
            all_reviews_in_page = []
            for m in movies:
                reviews = fetch_reviews_for_movie(m['id'], m['title'], max_pages=reviews_per_movie_pages)
                all_reviews_in_page.extend(reviews)

            if all_reviews_in_page:
                upload_to_minio(
                    data_list=all_reviews_in_page,
                    source="tmdb",
                    entity="reviews",
                    extraction_method="API Calling",
                    http_status=200,
                    search_params={"page_batch": page}
                )
                print(f"Đã đẩy {len(all_reviews_in_page)} reviews lên Bronze.")

        except Exception as e:
            print(f"Lỗi  tại trang {page}: {e}")

if __name__ == "__main__":
    ingest_tmdb_data(movie_pages=2, reviews_per_movie_pages=1)