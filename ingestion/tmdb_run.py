from concurrent.futures import ThreadPoolExecutor
from ingestion.tmdb.movies import ingest_tmdb_movie, fetch_trending_movie_ids
from ingestion.tmdb.reviews import ingest_tmdb_reviews
from ingestion.common.upload_data import upload_to_minio

def process_single_tmdb_movie(movie_id, max_reviews):
    movie_data = ingest_tmdb_movie(movie_id)
    r_status = ingest_tmdb_reviews(movie_id, max_reviews)
    return movie_data

def main(movie_limit=100, max_reviews=2000, max_workers=5):
    print(f"Khởi động TMDB Ingestion Pipeline (Đa luồng)...")

    # Bước 1: Lấy danh sách ID phim mục tiêu
    movie_ids = fetch_trending_movie_ids(limit=movie_limit)
    if not movie_ids:
        return
    print(f"Đang xử lý {len(movie_ids)} phim với {max_workers} luồng...")

    # Bước 2: Chạy đa luồng
    changed_movies = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # executor.submit để gửi các task vào pool
        futures = [
            executor.submit(process_single_tmdb_movie, mid, max_reviews) 
            for mid in movie_ids
        ]
        # Gom kết quả
        for future in futures:
            movie_data = future.result()
            if movie_data:
                changed_movies.append(movie_data)

    if changed_movies:
        upload_to_minio(
            raw_data=changed_movies,
            source="tmdb",
            entity="movies",
            methods="Batch Execution API",
            http_status=200,
            search_params={"movie_count": len(changed_movies)}
        )
        print(f"Đã upload {len(changed_movies)} phim TMDB lên MinIO thành công trong 1 file raw_movies.jsonl duy nhất!")

    print("\nTMDB Pipeline hoàn tất!")

if __name__ == "__main__":
    # movie_limit: số phim, max_reviews: số bình luận tối đa
    main(movie_limit=100, max_reviews=200, max_workers=5)