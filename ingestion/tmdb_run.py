from concurrent.futures import ThreadPoolExecutor
from tmdb.movies import ingest_tmdb_movie, fetch_trending_movie_ids
from tmdb.reviews import ingest_tmdb_reviews

def process_single_tmdb_movie(movie_id, reviews_pages):
    m_status = ingest_tmdb_movie(movie_id)
    r_status = ingest_tmdb_reviews(movie_id, max_pages=reviews_pages)
    return f"{m_status} | {r_status}"

def main(movie_limit=10, reviews_pages=2, max_workers=5):
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
            executor.submit(process_single_tmdb_movie, mid, reviews_pages) 
            for mid in movie_ids
        ]
        # In kết quả khi từng thread hoàn thành
        for future in futures:
            print(f"{future.result()}")

    print("\nTMDB Pipeline hoàn tất!")

if __name__ == "__main__":
    # movie_limit: số phim, reviews_pages: số trang review (mỗi trang ~20 bản ghi)
    main(movie_limit=20, reviews_pages=2, max_workers=5)