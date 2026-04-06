import sys
import os
from gold.elt.load_dimensions import load_dimensions
from gold.elt.load_facts import load_fact_reviews

# Nếu hàm xử lý phim của bạn có tên khác (ví dụ: load_movie_to_db), hãy đổi tên cho khớp nhé
# from gold.etl.load_dimensions import load_movie_to_db 

def run_gold_layer(target_date):
    print(f"\n=======================================================")
    print(f"🚀 BẮT ĐẦU CHẠY PIPELINE TẦNG GOLD - NGÀY {target_date}")
    print(f"=======================================================\n")

    try:
        # BƯỚC 1: Nạp các bảng Dimension cơ bản không có phụ thuộc
        print(">>> STAGE 1: NẠP DIMENSIONS (Date, Review Text) <<<")
        load_dimensions(target_date)

        # BƯỚC 2: Nạp Fact Table (BẮT BUỘC CHẠY CUỐI CÙNG)
        print("\n>>> STAGE 2: NẠP FACT REVIEWS <<<")
        load_fact_reviews(target_date)

        print(f"\n=======================================================")
        print(f"✅ HOÀN TẤT THÀNH CÔNG TẦNG GOLD - NGÀY {target_date}")
        print(f"=======================================================\n")

    except Exception as e:
        print(f"\n[!] ❌ LỖI NGHIÊM TRỌNG TRONG QUÁ TRÌNH CHẠY TẦNG GOLD: {e}")
        # Báo lỗi ra hệ thống để Airflow biết Task này đã thất bại (Red Task)
        sys.exit(1)

if __name__ == "__main__":
    # Lấy ngày tháng từ tham số dòng lệnh (Airflow sẽ truyền vào)
    if len(sys.argv) < 2:
        print("Sử dụng: python gold_run.py <YYYY-MM-DD>")
        print("Ví dụ: python gold_run.py 2026-03-30")
        sys.exit(1)
        
    target_date_arg = sys.argv[1]
    run_gold_layer(target_date_arg)