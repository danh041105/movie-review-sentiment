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
    imdb_path = f"s3a://{SILVER_BUCKET}/reviews/imdb/{date_path}/*.parquet"
    tmdb_path = f"s3a://{SILVER_BUCKET}/reviews/tmdb/{date_path}/*.parquet"
    print(f"[*] Đang nạp dữ liệu từ: {imdb_path} và {tmdb_path}")
    # Đọc từng nguồn riêng, bỏ qua nếu không có dữ liệu mới (Redis dedup skip hết)
    dfs = []
    try:
        imdb_df = spark.read.parquet(imdb_path)
        dfs.append(imdb_df)
        print(f"[+] IMDB: nạp thành công")
    except Exception as e:
        print(f"[!] IMDB: không có dữ liệu mới cho ngày {target_date} (skip)")

    try:
        tmdb_df = spark.read.parquet(tmdb_path)
        dfs.append(tmdb_df)
        print(f"[+] TMDB: nạp thành công")
    except Exception as e:
        print(f"[!] TMDB: không có dữ liệu mới cho ngày {target_date} (skip)")

    # Nếu cả 2 nguồn đều không có dữ liệu → dừng lại, không crash
    if not dfs:
        print(f"[!] Không có reviews mới từ bất kỳ nguồn nào cho ngày {target_date}. Bỏ qua merge.")
        spark.stop()
        return

    # Union các nguồn có dữ liệu
    from functools import reduce
    full_df = reduce(lambda a, b: a.unionByName(b), dfs).select(
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