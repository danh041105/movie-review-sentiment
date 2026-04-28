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

    # BƯỚC 1: ĐỌC DỮ LIỆU TỪ TẦNG SILVER
    silver_bucket = "silver"
    reviews_path = get_layer_path("s3a://", silver_bucket, "nlp/reviews_enriched", target_date)
    print(f"[*] Đang đọc dữ liệu review từ: {reviews_path}")
    try:
        reviews_df = spark.read.parquet(reviews_path + "*.parquet")
    except Exception as e:
        print(f"[!] Bỏ qua cập nhật Fact Table: Không tìm thấy file Parquet tại {reviews_path}. (Có thể do không có data review mới hôm nay).")
        spark.stop()
        return
    print("[*] Đang chuẩn bị các dimension keys và metrics...")
    facts_prepared_df = reviews_df.select(
        "review_id",
        "imdb_id",
        F.date_format("created_at", "yyyyMMdd").alias("date_id"),
        F.when(F.col("source_system") == "imdb", 1).otherwise(2).alias("source_id"),
        F.col("predicted_sentiment").cast("int").alias("sentiment_id"),
        F.col("rating").cast("float").alias("rating"),
        F.col("sentiment_confidence").cast("float").alias("sentiment_confidence"),
        F.col("created_at").alias("created_date"),
        F.col("ingestion_id")
    )

    # BƯỚC 3: TRA CỨU MOVIE_ID TỪ POSTGRESQL (LOOK-UP)
    print("[*] Đang kết nối Database để tra cứu surrogate keys...")
    jdbc_url = get_jdbc_url()
    db_props = get_postgres_properties()
    
    # Chỉ đọc đúng 2 cột cần thiết từ dim_movie để giảm tải bộ nhớ
    dim_movie_db = spark.read.jdbc(url=jdbc_url, table="dim_movie", properties=db_props) \
                        .select("movie_id", "imdb_id")
                        

    # BƯỚC 4: JOIN VÀ CHỐT KHUNG DỮ LIỆU CUỐI CÙNG
    # Sử dụng F.broadcast cho dim_movie vì bảng chiều này thường nhỏ gọn, 
    # giúp tăng tốc độ Join lên rất nhiều lần!
    fact_reviews_final = facts_prepared_df.join(
        F.broadcast(dim_movie_db), 
        on="imdb_id",
        how="inner"
    ).select(
        "review_id",
        "movie_id",
        "date_id",
        "source_id",
        "sentiment_id",
        "rating",
        "sentiment_confidence",
        "created_date",
        "ingestion_id"
    )

    # BƯỚC 5: LOẠI TRÙNG VỚI DỮ LIỆU ĐÃ CÓ (Lưới an toàn cho Checkpoint)
    # Vì Checkpoint dùng dấu < (cào lại ngày checkpoint để không sót bài),
    # một số review của ngày checkpoint có thể bị trùng → cần loại ở đây.

    print("[*] Đang kiểm tra review_id trùng lặp với dữ liệu đã có...")
    existing_review_ids = spark.read.jdbc(
        url=jdbc_url, table="fact_reviews", properties=db_props
    ).select("review_id")
    
    new_facts = fact_reviews_final.join(existing_review_ids, on="review_id", how="left_anti")
    
    new_count = new_facts.count()
    skipped = fact_reviews_final.count() - new_count
    print(f"[*] Reviews mới: {new_count} | Trùng lặp bỏ qua: {skipped}")
    # BƯỚC 6: GHI VÀO POSTGRESQL
    if new_count > 0:
        print(f"[*] Đang nạp {new_count} sự kiện mới vào fact_reviews...")
        write_to_postgres(new_facts, "fact_reviews", mode="append")
    else:
        print("[*] Không có review mới. Bỏ qua.")
    print("========== HOÀN TẤT NẠP FACT_REVIEWS ==========")
    spark.stop()

if __name__ == "__main__":
    target_date = datetime.now()
    load_fact_reviews(target_date)