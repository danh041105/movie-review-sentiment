import os
import sys
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.window import Window

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from transformation.common.spark_utils import get_spark_session, write_data_to_minio
from transformation.common.schema import Movie_Schema

SILVER_BUCKET = "silver"

def merge_movies(target_date=None):
    spark = get_spark_session("merge_movies")  
    if target_date is None:
        target_date = datetime.now().strftime("%Y-%m-%d")
    
    # 1. Đọc toàn bộ lịch sử Silver của phim (Dimension data cực kỳ hợp lý để đọc hết)
    # Vì phim kéo về từ các ngày khác nhau, nếu chỉ đọc hôm nay sẽ bị lệch thông tin khi JOIN
    imdb_path = f"s3a://{SILVER_BUCKET}/imdb/movies/*/*/*/*.parquet" 
    tmdb_path = f"s3a://{SILVER_BUCKET}/tmdb/movies/*/*/*/*.parquet"
    
    print(f"[*] Đang nạp toàn bộ lịch sử dữ liệu từ: {imdb_path} và {tmdb_path}")
    
    try:
        imdb_df = spark.read.parquet(imdb_path)
    except Exception as e:
        print("[!] Không có dữ liệu IMDB Movie")
        imdb_df = None

    try:
        tmdb_df = spark.read.parquet(tmdb_path)
    except Exception as e:
        print("[!] Không có dữ liệu TMDB Movie")
        tmdb_df = None

    if imdb_df is None and tmdb_df is None:
        print("[!] Không có dữ liệu từ cả 2 nguồn, quá trình merge bị hủy!")
        spark.stop()
        return

    # Gộp dữ liệu — JOIN theo imdb_id, ưu tiên IMDB cho các trường chung
    final_df = imdb_df.alias("i").join(tmdb_df.alias("t"), ["imdb_id"], "full_outer") \
        .select(
            F.coalesce(F.col("i.ingestion_id"), F.col("t.ingestion_id")).alias("ingestion_id"),
            F.coalesce(F.col("i.source_system"), F.col("t.source_system")).alias("source_system"),
            F.greatest(F.col("i.ingestion_timestamp"), F.col("t.ingestion_timestamp")).alias("ingestion_timestamp"),
            F.col("imdb_id"),
            F.col("t.tmdb_id"),
            F.coalesce(F.col("i.title"), F.col("t.title")).alias("title"),
            F.col("t.original_title"),
            F.coalesce(F.col("i.genres"), F.col("t.genres")).alias("genres"),
            F.col("t.original_language"),
            F.coalesce(F.col("i.release_date"), F.col("t.release_date")).alias("release_date"),
            F.coalesce(F.col("i.duration_seconds"), F.col("t.duration_seconds")).alias("duration_seconds"),
            F.col("t.tmdb_vote_average"),
            F.col("t.tmdb_vote_count"),
            F.col("i.imdb_rating"),
            F.col("i.imdb_vote_count"),
            F.col("t.overview"),
            F.col("i.description"),
            F.col("i.production_budget"),
            F.col("i.worldwide_gross"),
            F.col("t.budget"),
            F.col("t.revenue"),
        )

    final_df = final_df.filter(F.col("imdb_id").isNotNull())
    validated_columns = [
        F.col(field.name).cast(field.dataType).alias(field.name)
        for field in Movie_Schema.fields
    ]
    final_silver_df = final_df.select(*validated_columns)

    write_data_to_minio(final_silver_df, SILVER_BUCKET, "merged_movies", target_date, mode="overwrite")
    print(f"Tổng số bản ghi Master Movies tạo ra: {final_silver_df.count()}")
    spark.stop()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default=None)
    args = parser.parse_args()
    target_date = args.date if args.date else datetime.now().strftime("%Y-%m-%d")
    merge_movies(target_date)
