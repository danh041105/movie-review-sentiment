import os
import sys
from datetime import datetime
from pyspark.sql import functions as F
from transformation.common.spark_utils import get_spark_session, write_data_to_minio
SILVER_BUCKET = "silver"
def create_training_dataset(target_date=None):
    spark = get_spark_session("merge_reviews")  
    if target_date is None:
        target_date = datetime.now().strftime("%Y-%m-%d")
    date_path = target_date.replace("-", "/")
    imdb_path = f"s3a://{SILVER_BUCKET}/reviews/imdb/{date_path}/*.snappy.parquet"
    tmdb_path = f"s3a://{SILVER_BUCKET}/reviews/tmdb/{date_path}/*.snappy.parquet"
    
    print(f"[*] Đang nạp dữ liệu từ: {imdb_path} và {tmdb_path}")
    
    imdb_df = spark.read.parquet(imdb_path)
    tmdb_df = spark.read.parquet(tmdb_path)
    
    full_df = imdb_df.unionByName(tmdb_df).select(
        "review_id",
        "tmdb_id", 
        "imdb_id",
        "author", 
        "content", 
        "rating",
        "source_system",
        "created_at",
        "ingestion_id"
    )
    # 3. Làm sạch dữ liệu cấp độ Dataset
    # - Loại bỏ trùng lặp dựa trên review_id
    # - Lọc review có độ dài > 30 ký tự để đảm bảo có đủ ngữ cảnh cho khía cạnh
    clean_df = full_df.dropDuplicates(["review_id"]) \
                      .filter(F.length(F.col("content")) > 30) \
                      .filter(F.col("content").isNotNull())
    
    # 4. Lưu dữ liệu ra phân vùng trung gian chờ Hugging Face nạp
    write_data_to_minio(clean_df, SILVER_BUCKET, "nlp/dataset", target_date)
    print(f"Tổng số bản ghi chuẩn bị: {clean_df.count()}")
    clean_df.groupBy("source_system").count().show()
    spark.stop()

if __name__ == "__main__":
    target_date = datetime.now().strftime("%Y-%m-%d")
    create_training_dataset(target_date)