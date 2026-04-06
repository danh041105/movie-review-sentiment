from concurrent.futures import ThreadPoolExecutor
from ingestion.tmdb.movies import ingest_tmdb_movie, fetch_trending_movie_ids
from ingestion.tmdb.reviews import ingest_tmdb_reviews

def process_single_tmdb_movie(movie_id, max_reviews):
    m_status = ingest_tmdb_movie(movie_id)
    r_status = ingest_tmdb_reviews(movie_id, max_reviews)
    return f"{m_status} | {r_status}"

def main(movie_limit=100, max_reviews=2000, max_workers=5):
    print(f"Khởi động TMDB Ingestion Pipeline (Đa luồng)...")

    # Bước 1: Lấy danh sách ID phim mục tiêu
    movie_ids = fetch_trending_movie_ids(limit=movie_limit)
    if not movie_ids:
        return
    print(f"Đang xử lý {len(movie_ids)} phim với {max_workers} luồng...")

    # Bước 2: Chạy đa luồng
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # executor.submit để gửi các task vào pool
        futures = [
            executor.submit(process_single_tmdb_movie, mid, max_reviews) 
            for mid in movie_ids
        ]
        # In kết quả khi từng thread hoàn thành
        for future in futures:
            print(f"{future.result()}")

    print("\nTMDB Pipeline hoàn tất!")

if __name__ == "__main__":
    # movie_limit: số phim, max_reviews: số bình luận tối đa
    main(movie_limit=100, max_reviews=200, max_workers=5)