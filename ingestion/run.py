from concurrent.futures import ThreadPoolExecutor
# Import trực tiếp vì các file nằm cùng thư mục imdb/
from imdb.movies import fetch_movie_ids, ingest_imdb_movies 
from imdb.reviews import ingest_reviews_movie

def process_single_movie(movie_id, reviews_per_movie):
    """
    Worker xử lý trọn gói cho 1 phim: 
    Lấy Metadata và Reviews rồi đẩy lên MinIO.
    """
    try:
        ingest_imdb_movies(movie_id)
        ingest_reviews_movie(movie_id, reviews_per_movie)
        return f"Thành công: {movie_id}"
    except Exception as e:
        return f"Lỗi tại {movie_id}: {str(e)}"
    
def main(movie_limit, reviews_per_movie, max_workers=5):
    print(f"Bắt đầu IMDb Ingestion Pipeline (Parallel Mode)...")
    all_ids = fetch_movie_ids()
    if not all_ids:
        print("Lỗi: Không lấy được danh sách Movie ID.")
        return
    
    target_ids = all_ids[:movie_limit]
    print(f"Quy mô: {len(target_ids)} phim | Số luồng: {max_workers}")

    # Bước 2: Phân phối ID vào Pool để chạy đa luồng
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # executor.map sẽ gửi từng ID vào hàm process_single_movie
        results = list(executor.map(lambda mid: process_single_movie(mid, reviews_per_movie), target_ids))
        
    # In báo cáo kết quả sau khi hoàn thành
    print("\n--- BÁO CÁO KẾT QUẢ ---")
    for res in results:
        print(f"✅ {res}")

    print("\nPipeline hoàn tất thành công!")

if __name__ == "__main__":
    # movie_limit: Tổng số phim muốn cào
    # reviews_per_movie: Số lượng review tối đa cho mỗi phim
    # max_workers: Số luồng chạy song song (Nên để 5-10)
    main(movie_limit=20, reviews_per_movie=100, max_workers=5)