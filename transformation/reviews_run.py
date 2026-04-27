import sys
import os
import argparse
from datetime import datetime
# Thêm thư mục gốc vào sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from transformation.imdb.reviews_transform import transform_imdb_reviews
from transformation.tmdb.reviews_transform import transform_tmdb_reviews

def run_all_reviews_transformation(target_date=None):
    if target_date is None:
        target_date = datetime.now().strftime("%Y-%m-%d")
    print(f"========== BẮT ĐẦU CHẠY PIPELINE REVIEWS (SILVER) - NGÀY: {target_date} ==========\n")
    # 1. CHẠY LUỒNG IMDB REVIEWS
    print(f"[1/1] Đang khởi động tiến trình biến đổi dữ liệu TMDb Reviews...")
    try:
        transform_tmdb_reviews(target_date)
        print(f"[+] Hoàn tất luồng TMDb Reviews!\n")
    except Exception as e:
        print(f"[!] Lỗi nghiêm trọng khi chạy TMDb Reviews: {e}\n")
    # 2. CHẠY LUỒNG TMDB REVIEWS
    print(f"[2/2] Đang khởi động tiến trình biến đổi dữ liệu IMDB Reviews...")
    try:
        transform_imdb_reviews(target_date)
        print(f"[+] Hoàn tất luồng IMDb Reviews!\n")
    except Exception as e:
        print(f"[!] Lỗi nghiêm trọng khi chạy IMDb Reviews: {e}\n")
    print(f"========== KẾT THÚC PIPELINE REVIEWS ==========\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Chạy luồng xử lý dữ liệu Bình luận (Reviews)")
    parser.add_argument(
        "--date", 
        type=str, 
        default=None, 
        help="Ngày cần chạy bù (Backfill) theo định dạng YYYY-MM-DD. Mặc định là hôm nay."
    )
    args = parser.parse_args()
    
    run_all_reviews_transformation(args.date)