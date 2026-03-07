import os
import sys
import requests
from datetime import datetime, timedelta
import time
from dotenv import load_dotenv
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from upload_data import upload_to_minio

load_dotenv()
TMDB_TOKEN = os.getenv("TMDB_READ_ACCESS_TOKEN")
HEADERS = {
    "accept": "application/json",
    "Authorization": f"Bearer {TMDB_TOKEN}"
}

def fetch_reviews_batch(movies_list, reviews_page):
    all_reviews_buffer = []
    for movie in movies_list:
        movie_id = movie.get("id")
        movie_title = movie.get("title")
        for page in range(1, reviews_page + 1):
            url = f"https://api.themoviedb.org/3/movie/{movie_id}/reviews"
            try:
                response = requests.get(url=url, headers=HEADERS, params={"page": page})
                if response.status_code == 200:
                    reviews = response.json().get("results", [])
                    if not reviews: 
                        break

                    for review in reviews:
                        review["movie_id"] = movie_id
                        review["movie_title"] = movie_title
                        all_reviews_buffer.append(review)   
                    if page >= response.json().get("total_pages", 1):
                            break
                time.sleep(0.05)

            except Exception as e:
                print(f"Lỗi lấy review phim {movie_title} trang {page}: {e}")
                break
    return all_reviews_buffer

def ingest_tmdb_data(movie_pages, reviews_pages):
    discover_url = "https://api.themoviedb.org/3/discover/movie"
    today = datetime.now()
    last_month = today - timedelta(days=30)
    for page in range(1, movie_pages + 1):
        params = {
            "language": "en-US",
            "sort_by": "popularity.desc",
            "primary_release_date.gte": last_month.strftime('%Y-%m-%d'),
            "page": page
        }
        try:
            print(f"Đang xử lý Batch trang {page}...")
            response = requests.get(discover_url, headers=HEADERS, params=params)
            if response.status_code != 200: continue
            movies = response.json().get("results", [])
            if movies:
                # --- THỰC HIỆN BATCH UPLOAD ---
                # A. Upload Metadata phim của trang này (1 file duy nhất cho 20 phim)
                upload_to_minio(
                    movies=movies, 
                    source="tmdb", 
                    entity="movies", 
                    http_status=200, 
                    search_params={"page": page}
                )
                # B. Thu thập toàn bộ reviews của 20 phim này vào buffer
                print(f"Đang gom các bình luận cho {len(movies)} phim của trang {page}...")
                reviews_batch = fetch_reviews_batch(movies, reviews_pages)
                
                # C. Upload 1 file review duy nhất chứa hàng trăm comment
                if reviews_batch:
                    upload_to_minio(
                        movies=reviews_batch, 
                        source="tmdb", 
                        entity="reviews", 
                        http_status=200, 
                        search_params={"page": page, "type": "batch"})
                    print(f"✅ Đã đẩy {len(reviews_batch)} reviews lên Bronze.")
                
        except Exception as e:
            print(f"Lỗi nghiêm trọng tại trang {page}: {e}")
    
    all_movies_data = []
    for page in range(1, movie_pages + 1):
        # ... logic gọi requests ...
        movies = response.json().get("results", [])
        if movies:
            # Lưu metadata vào Bronze như cũ
            upload_to_minio(movies, "tmdb", "movies", 200, {"page": page})
            
            # Thu thập thông tin để trả về cho run.py
            for m in movies:
                all_movies_data.append({
                    "tmdb_id": m['id'],
                    "title": m['title'],
                    "slug": m['title'].lower().replace(" ", "-").replace(":", "")
                })
    return all_movies_data