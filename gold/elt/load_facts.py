import sys
import os
from datetime import datetime
from pyspark.sql import functions as F
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from transformation.common.spark_utils import get_spark_session, get_layer_path
from gold.db_utils import write_to_postgres, get_jdbc_url, get_postgres_properties

def load_fact_reviews(target_date=None):
    if target_date is None:
        target_date = datetime.now()
    print(f"\n========== BẮT ĐẦU NẠP FACT_REVIEWS - NGÀY {target_date} ==========")
    spark = get_spark_session("Gold_Load_Fact_Reviews")
    # =========================================================
    # BƯỚC 1: ĐỌC DỮ LIỆU TỪ TẦNG SILVER
    # =========================================================
    silver_bucket = "silver"
    reviews_path = get_layer_path("s3a://", silver_bucket, "nlp/reviews_enriched", target_date)
    print(f"[*] Đang đọc dữ liệu review từ: {reviews_path}")
    try:
        reviews_df = spark.read.parquet(reviews_path + "*.parquet")
    except Exception as e:
        print(f"[!] Bỏ qua cập nhật Fact Table: Không tìm thấy file Parquet tại {reviews_path}. (Có thể do không có data review mới hôm nay).")
        spark.stop()
        return
    
    # =========================================================
    # BƯỚC 2: CHUYỂN ĐỔI VÀ TẠO CÁC KHÓA (KEYS) CƠ BẢN
    # =========================================================
    print("[*] Đang chuẩn bị các dimension keys và metrics...")
    facts_prepared_df = reviews_df.select(
        "review_id",
        "imdb_id", # Giữ lại tạm thời để Lát nữa JOIN lấy movie_id
        # Tạo date_id theo đúng chuẩn YYYYMMDD
        F.date_format("created_at", "yyyyMMdd").alias("date_id"),
        # Ánh xạ source_system sang source_id (1: imdb, 2: tmdb)
        F.when(F.col("source_system") == "imdb", 1).otherwise(2).alias("source_id"),
        # Ép kiểu prediction từ Float (của MLlib) sang Int làm sentiment_id
        F.col("predicted_sentiment").cast("int").alias("sentiment_id"),
        # Đảm bảo các chỉ số đo lường đúng chuẩn kiểu Float
        F.col("rating").cast("float").alias("rating"),
        F.col("sentiment_confidence").cast("float").alias("sentiment_confidence"),
        # Lấy thông tin thời gian thực và ID luồng chạy
        F.col("created_at").alias("created_date"),
        F.col("ingestion_id")
    )
    # =========================================================
    # BƯỚC 3: TRA CỨU MOVIE_ID TỪ POSTGRESQL (LOOK-UP)
    # =========================================================
    print("[*] Đang kết nối Database để tra cứu surrogate keys...")
    jdbc_url = get_jdbc_url()
    db_props = get_postgres_properties()
    
    # Chỉ đọc đúng 2 cột cần thiết từ dim_movie để giảm tải bộ nhớ
    dim_movie_db = spark.read.jdbc(url=jdbc_url, table="dim_movie", properties=db_props) \
                        .select("movie_id", "imdb_id")
                        
    # =========================================================
    # BƯỚC 4: JOIN VÀ CHỐT KHUNG DỮ LIỆU CUỐI CÙNG
    # =========================================================
    print("[*] Đang ráp nối dữ liệu để tạo Fact hoàn chỉnh...")
    
    # Sử dụng F.broadcast cho dim_movie vì bảng chiều này thường nhỏ gọn, 
    # giúp tăng tốc độ Join lên rất nhiều lần!
    fact_reviews_final = facts_prepared_df.join(
        F.broadcast(dim_movie_db), 
        on="imdb_id",
        how="inner"
    ).select(
        "review_id",
        "movie_id", # Khóa thay thế quý giá lấy từ DB
        "date_id",
        "source_id",
        "sentiment_id",
        "rating",
        "sentiment_confidence",
        "created_date",
        "ingestion_id"
    )
    # =========================================================
    # BƯỚC 5: GHI VÀO POSTGRESQL
    # =========================================================
    print("[*] Đang nạp toàn bộ sự kiện vào fact_reviews...")
    # Vì bảng Fact lưu dữ liệu theo thời gian, chúng ta luôn dùng mode="append"
    write_to_postgres(fact_reviews_final, "fact_reviews", mode="append")
    print("========== HOÀN TẤT NẠP FACT_REVIEWS ==========")
    spark.stop()
if __name__ == "__main__":
    target_date = datetime.now()  # Nhớ thay đổi ngày cho khớp với dữ liệu test của bạn
    load_fact_reviews(target_date)