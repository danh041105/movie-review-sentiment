import time
from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from ingestion.imdb.movies import fetch_movie_ids, get_movies_detail, upload_movies_to_minio
from ingestion.imdb.reviews import ingest_reviews_movie

def main(movie_limit=100, reviews_per_movie=2000, max_workers=15):
    start_time = time.time()
    print(f"[START] IMDb Pipeline: {movie_limit} phim | {reviews_per_movie} reviews/phim")
    print("=" * 40)
    # -------------------------------------------------------
    # PHASE 1: Cào thông tin phim
    # -------------------------------------------------------
    print("\nPhase 1: Cào thông tin phim...")
    phase1_start = time.time()

    movie_ids = fetch_movie_ids(limit=movie_limit)
    if not movie_ids:
        print("Lỗi: Không lấy được danh sách Movie ID.")
        return

    all_movies = get_movies_detail(movie_ids)
    if not all_movies:
        print("Phase 1 thất bại: Không lấy được Metadata.")
        return

    upload_movies_to_minio(all_movies)
    phase1_duration = time.time() - phase1_start
    print(f"Phase 1 hoàn tất: {len(all_movies)} phim trong {round(phase1_duration, 2)}s\n")

    # -------------------------------------------------------
    # PHASE 2: Cào reviews phim
    # -------------------------------------------------------
    print(f"Phase 2: Cào reviews ({max_workers} luồng song song)...")
    phase2_start = time.time()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_movie = {
            executor.submit(ingest_reviews_movie, movie_id, reviews_per_movie): movie_id
            for movie_id in movie_ids
        }
        completed = 0
        for future in as_completed(future_to_movie):
            completed += 1
            result = future.result()
            print(f"[{completed}/{len(movie_ids)}] {result}")

    phase2_duration = time.time() - phase2_start
    print(f"Phase 2 hoàn tất: {len(movie_ids)} phim trong {round(phase2_duration, 2)}s\n")

    # -------------------------------------------------------
    # Tổng kết
    # -------------------------------------------------------
    total_duration = time.time() - start_time
    print("=" * 40)
    print(f"HOÀN TẤT PIPELINE")
    print(f"Tổng thời gian: {str(timedelta(seconds=int(total_duration)))} ({round(total_duration, 2)}s)")
    print(f"Trung bình reviews: {round(phase2_duration / len(movie_ids), 2)}s/phim")
    print("=" * 40)

if __name__ == "__main__":
    main(movie_limit=100, reviews_per_movie=2000, max_workers=15)