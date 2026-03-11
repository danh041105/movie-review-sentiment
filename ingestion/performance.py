import time
from concurrent.futures import ThreadPoolExecutor
from imdb.movies import fetch_movie_ids, ingest_imdb_movies
from imdb.reviews import ingest_reviews_movie

def process_single_movie(movie_id, reviews_per_movie):
    """
    Worker xử lý trọn gói 1 phim: Metadata + Reviews.
    Hàm này sẽ được gọi bởi cả vòng lặp tuần tự và đa luồng.
    """
    ingest_imdb_movies(movie_id)
    ingest_reviews_movie(movie_id, reviews_per_movie)

def run_sequential(movie_ids, reviews_per_movie):
    start = time.time()
    for mid in movie_ids:
        process_single_movie(mid, reviews_per_movie)
    return time.time() - start

def run_parallel(movie_ids, reviews_per_movie, workers):
    start = time.time()
    with ThreadPoolExecutor(max_workers=workers) as executor:
        executor.map(lambda mid: process_single_movie(mid, reviews_per_movie), movie_ids)
    return time.time() - start

def main(movie_limit=5, reviews_per_movie=50, max_workers=5):
    print(f" Bắt đầu đo hiệu năng IMDb Ingestion Pipeline...")
    all_ids = fetch_movie_ids()
    if not all_ids:
        print("❌ Lỗi: Không lấy được movie id.")
        return
    
    target_ids = all_ids[:movie_limit]
    print(f"Quy mô thử nghiệm: {len(target_ids)} phim | {reviews_per_movie} reviews/phim\n")

    # 1. Đo thời gian chạy tuần tự
    print("⏳ Đang thực hiện chạy TUẦN TỰ (Sequential)...")
    seq_time = run_sequential(target_ids, reviews_per_movie)
    print(f"✔️ Hoàn thành tuần tự trong: {seq_time:.2f} giây\n")

    # 2. Đo thời gian chạy đa luồng
    print(f"Đang thực hiện chạy ĐA LUỒNG (Parallel - {max_workers} threads)...")
    par_time = run_parallel(target_ids, reviews_per_movie, max_workers)
    print(f"Hoàn thành đa luồng trong: {par_time:.2f} giây\n")

    # 3. Phân tích kết quả
    speedup = seq_time / par_time
    improvement = (1 - par_time / seq_time) * 100
    print("="*50)
    print(f"📊 BÁO CÁO HIỆU NĂNG (BENCHMARK):")
    print(f"  - Thời gian tuần tự: {seq_time:.2f}s")
    print(f"  - Thời gian đa luồng: {par_time:.2f}s")
    print(f"  - Tốc độ nhanh gấp:  {speedup:.2f} lần")
    print(f"  - Hiệu quả cải thiện: {improvement:.1f}%")
    print("="*50)
    print("💡 Lời khuyên: Bạn có thể tăng max_workers lên 10 nếu mạng ổn định.")
if __name__ == "__main__":
    main(movie_limit=20, reviews_per_movie=100, max_workers=5)