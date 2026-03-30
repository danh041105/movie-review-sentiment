import sys
import argparse
from datetime import datetime
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from transformation.tmdb.movies_transform import transform_tmdb_movies
from transformation.imdb.movies_transform import transform_imdb_movies

def run_all_movies_transformation(target_date=None):
    """
    Hàm điều phối: Chạy toàn bộ pipeline xử lý dữ liệu Movies từ tầng Bronze lên Silver.
    """
    if target_date is None:
        target_date = datetime.now().strftime("%Y-%m-%d")
    print(f"========== BẮT ĐẦU CHẠY PIPELINE MOVIES (SILVER) - NGÀY: {target_date} ==========\n")
    # ---------------------------------------------------------
    # 1. CHẠY LUỒNG TMDB MOVIES
    # ---------------------------------------------------------
    print(f"[1/2] Đang khởi động tiến trình biến đổi dữ liệu TMDB Movies...")
    try:
        transform_tmdb_movies(target_date)
        print(f"[+] Hoàn tất luồng TMDB Movies!\n")
    except Exception as e:
        print(f"[!] Lỗi nghiêm trọng khi chạy TMDB Movies: {e}\n")

    # ---------------------------------------------------------
    # 2. CHẠY LUỒNG IMDB MOVIES
    # ---------------------------------------------------------
    print(f"[2/2] Đang khởi động tiến trình biến đổi dữ liệu IMDb Movies...")
    try:
        transform_imdb_movies(target_date)
        print(f"[+] Hoàn tất luồng IMDb Movies!\n")
    except Exception as e:
        print(f"[!] Lỗi nghiêm trọng khi chạy IMDb Movies: {e}\n")

    print(f"========== KẾT THÚC PIPELINE MOVIES ==========\n")


# ... (các import khác của bạn)

if __name__ == "__main__":
    # 1. Khởi tạo bộ đọc tham số
    parser = argparse.ArgumentParser(description="Chạy luồng xử lý dữ liệu Phim (Movies)")
    # 2. Định nghĩa tham số '--date'
    parser.add_argument(
        "--date", 
        type=str, 
        default=None, 
        help="Ngày cần chạy bù (Backfill) theo định dạng YYYY-MM-DD. Nếu bỏ trống sẽ tự chạy ngày hôm nay."
    )
    # 3. Lệnh cho Python đọc những gì người dùng gõ trên Terminal
    args = parser.parse_args()
    # 4. Kích hoạt chạy pipeline với ngày lấy được
    run_all_movies_transformation(args.date)