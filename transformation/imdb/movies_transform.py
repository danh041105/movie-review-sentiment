import os, sys
from datetime import datetime
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from transformation.common.spark_utils import get_spark_session, read_daily_data_from_minio, write_data_to_minio
from transformation.common.schema import IMDB_Movie_Schema
from dotenv import load_dotenv
load_dotenv()

def transform_imdb_movies(target_date = None):
    spark = get_spark_session("IMDB_Movies_Silver_Transform")
    raw_df = read_daily_data_from_minio(spark, "bronze", "imdb/movies", target_date)
    if raw_df is None:
        print("[*] Không có dữ liệu để xử lý trong ngày này.")
        spark.stop()
        return
    # chuẩn hóa dữ liệu 
    transform_df = raw_df.select(
        F.col("metadata.ingestion_id").alias("ingestion_id"),
        F.col("metadata.source_system").alias("source_system"),
        F.col("metadata.ingestion_timestamp").cast("timestamp").alias("ingestion_timestamp"),
        F.col("metadata.raw_hash").alias("raw_hash"),
        
        F.col("raw_payload.imdb_id").alias("imdb_id"),
        F.col("raw_payload.title").alias("title"),
        F.make_date(
        F.col("raw_payload.release_date.year"),
        F.col("raw_payload.release_date.month"),
        F.col("raw_payload.release_date.day"),
        ).alias("release_date"),
        F.col("raw_payload.genres").alias("genres"),
        F.col("raw_payload.description").alias("description"),
        (F.col("raw_payload.duration_seconds")).alias("duration_seconds"),
        F.col("raw_payload.rating").cast("double").alias("imdb_rating"),
        F.col("raw_payload.vote_count").cast("long").alias("imdb_vote_count"),
    ).filter(F.col("imdb_id").isNotNull())

    # Xử lý trùng lặp theo imdb_id
    window_spec = Window.partitionBy("imdb_id").orderBy(F.desc("ingestion_timestamp"))
    dedup_df = transform_df.withColumn("rn", F.row_number().over(window_spec)) \
                           .filter(F.col("rn") == 1) \
                           .drop("rn")
    
    validated_columns = [
        F.col(field.name).cast(field.dataType).alias(field.name)
        for field in IMDB_Movie_Schema.fields
    ]
    final_silver_df = dedup_df.select(*validated_columns)
    # 5. Ghi dữ liệu xuống tầng Silver (Tận dụng spark_utils)
    write_data_to_minio(final_silver_df, "silver", "imdb/movies", target_date)    
    spark.stop()
