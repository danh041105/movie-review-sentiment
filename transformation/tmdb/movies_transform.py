import os, sys
from datetime import datetime
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from transformation.common.spark_utils import get_spark_session, read_daily_data_from_minio, write_data_to_minio
from transformation.common.schema import TMDB_Movie_Schema
from dotenv import load_dotenv

load_dotenv()

def transform_tmdb_movies(target_date=None):
    spark = get_spark_session('TMDB_Movies_Silver_Transform')
    # 1. Đọc dữ liệu (Tận dụng spark_utils)
    raw_df = read_daily_data_from_minio(spark, "bronze", "tmdb/movies", target_date)
    if raw_df is None:
        print("[*] Không có dữ liệu để xử lý trong ngày này.")
        spark.stop()
        return
    # 2. Biến đổi cấu trúc và ép kiểu rỗng (Null) cho các trường của IMDb
    transformed_df = raw_df.select(
        F.col("metadata.ingestion_id").alias("ingestion_id"),
        F.col("metadata.source_system").alias("source_system"),
        F.col("metadata.ingestion_timestamp").cast("timestamp").alias("ingestion_timestamp"),
        F.col("metadata.raw_hash").alias("raw_hash"),
        
        F.col("raw_payload.id").cast("string").alias("tmdb_id"),
        F.col("raw_payload.imdb_id").alias("imdb_id"),
        F.trim(F.col("raw_payload.title")).alias("title"),
        F.col("raw_payload.original_title").alias("original_title"),
        F.col("raw_payload.genres").alias("genres"),
        F.col("raw_payload.original_language").alias("original_language"),
        F.to_date(F.col("raw_payload.release_date")).alias("release_date"),
        (F.col("raw_payload.runtime") * 60).cast("long").alias("duration_seconds"),
        F.round(F.col("raw_payload.vote_average"), 2).cast("double").alias("tmdb_vote_average"),
        F.col("raw_payload.vote_count").cast("long").alias("tmdb_vote_count"),
        F.trim(F.col("raw_payload.overview")).alias("overview"),
        F.col("raw_payload.budget").alias("budget"),
        F.col("raw_payload.revenue").alias("revenue"),
    ).dropna(subset=["tmdb_id", "title", "release_date", "tmdb_vote_average"])

    # 3. Deduplication: Lọc bản ghi mới nhất theo imdb_id
    window_spec = Window.partitionBy("imdb_id").orderBy(F.desc("ingestion_timestamp"))
    dedup_df = transformed_df.withColumn("rn", F.row_number().over(window_spec)) \
                             .filter(F.col("rn") == 1) \
                             .drop("rn")
    # 4. Ép khuôn Schema (Validate)
    validated_columns = [
        F.col(field.name).cast(field.dataType).alias(field.name)
        for field in TMDB_Movie_Schema.fields
    ]
    final_silver_df = dedup_df.select(*validated_columns)
    # 5. Ghi dữ liệu xuống tầng Silver (Tận dụng spark_utils)
    write_data_to_minio(final_silver_df, "silver", "tmdb/movies", target_date)    
    spark.stop()
