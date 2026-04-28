import os
import sys
from dotenv import load_dotenv
import json
import requests
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from ingestion.common.upload_data import upload_to_minio
from ingestion.common.redis_utils import get_review_checkpoint, save_review_checkpoint

load_dotenv()
TMDB_API_KEY = os.getenv("TMDB_API_KEY")

def get_imdb_id(movie_id):
    url = f"https://api.themoviedb.org/3/movie/{movie_id}"
    params = {"api_key": TMDB_API_KEY}
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    return data.get("imdb_id")

def get_movie_reviews(movie_id, max_reviews, checkpoint_date=None):
    """
    Cào reviews từ TMDb API với cơ chế Checkpoint (Early Stop).

    Args:
        movie_id: ID phim trên TMDb
        max_reviews: Số lượng review tối đa
        checkpoint_date: Ngày checkpoint từ Redis. 
                         Nếu gặp review có ngày <= checkpoint → DỪNG.

    Returns:
        list: Danh sách chỉ chứa các review MỚI hơn checkpoint.
    """
    page = 1
    all_reviews = []
    hit_checkpoint = False
    try:
        imdb_id = get_imdb_id(movie_id)
        while len(all_reviews) < max_reviews and not hit_checkpoint:
            url = f"https://api.themoviedb.org/3/movie/{movie_id}/reviews"
            params = {
                "api_key": TMDB_API_KEY,
                "page": page
            }
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                results = data.get("results", [])
                if not results: break
                for review in results:
                    # TMDb trả về "created_at" dạng "2026-04-27T12:00:00.000Z"
                    # Cắt lấy phần ngày "2026-04-27" để so sánh với checkpoint
                    review_date = review.get("created_at", "")[:10]
                    
                    # === CHECKPOINT: Dừng sớm nếu gặp review CŨ HƠN mốc ===
                    if checkpoint_date and review_date and review_date < checkpoint_date:
                        hit_checkpoint = True
                        print(f"[Checkpoint] TMDb {movie_id}: Gặp review ngày {review_date} <= checkpoint {checkpoint_date} → Dừng.")
                        break
                    
                    review["movie_id"] = movie_id
                    review["imdb_id"] = imdb_id
                    all_reviews.append(review)
                if page >= data.get("total_pages", 0): break
            page += 1
    except Exception as e:
        return f"Reviews TMDB Error {movie_id}: {e}"
    return all_reviews


def ingest_tmdb_reviews(movie_id, max_reviews):
    """
    Hàm chính để cào reviews TMDb với cơ chế Checkpoint Incremental.

    Flow:
      1. Đọc checkpoint từ Redis
      2. Cào reviews, dừng sớm khi gặp review cũ hơn checkpoint
      3. Nếu có review mới → Upload + Cập nhật checkpoint
      4. Nếu không có → Skip
    """
    # Bước 1: Đọc checkpoint
    checkpoint_date = get_review_checkpoint("tmdb", str(movie_id))
    if checkpoint_date:
        print(f"[Checkpoint] TMDb {movie_id}: Chỉ cào reviews sau ngày {checkpoint_date}")
    else:
        print(f"[Checkpoint] TMDb {movie_id}: Lần đầu cào → Full scrape")

    # Bước 2: Cào reviews (có early stop)
    new_reviews = get_movie_reviews(movie_id, max_reviews, checkpoint_date=checkpoint_date)

    # Nếu trả về string nghĩa là có lỗi
    if isinstance(new_reviews, str):
        return new_reviews

    if not new_reviews:
        return f"Reviews TMDB SKIP (không có review mới): {movie_id}"

    try:
        # Bước 3: Upload CHỈ những review mới
        upload_to_minio(
            raw_data=new_reviews,
            source="tmdb",
            entity="reviews",
            methods="API Calling",
            http_status=200,
            search_params={"movie_id": movie_id}
        )

        # Bước 4: Cập nhật checkpoint = ngày review mới nhất
        # TMDb trả về "created_at" dạng "2026-04-27T12:00:00.000Z"
        latest_date = new_reviews[0].get("created_at", "")[:10]
        if latest_date:
            save_review_checkpoint("tmdb", str(movie_id), latest_date, review_count=len(new_reviews))

        return f"Reviews TMDB OK (incremental): {movie_id} ({len(new_reviews)} reviews mới)"
    except Exception as e:
        return f"Reviews TMDB Error {movie_id}: {e}"

# if __name__ == "__main__":
#     movie_details = get_movie_reviews("687163", 200)
#     movie_details_json = json.dumps(movie_details, indent=4)
#     print(movie_details_json)