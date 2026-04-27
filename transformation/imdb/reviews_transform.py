import os
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from transformation.common.spark_utils import (get_spark_session, 
                                               read_daily_data_from_minio,
                                               write_data_to_minio)
from transformation.common.schema import Review_Schema


def transform_imdb_reviews(target_date=None):
    spark = get_spark_session("IMDB_Reviews_Silver_Transform")
    raw_df = read_daily_data_from_minio(spark, "bronze", "imdb/reviews", target_date)
    if raw_df is None:
        print("[*] Không có dữ liệu để xử lý trong ngày này.")
        spark.stop()
        return
    # chuẩn hóa dữ liệu 
    transform_df = raw_df.select(
        F.col("metadata.ingestion_id").alias("ingestion_id"),
        F.col("metadata.source_system").alias("source_system"),
        F.col("metadata.ingestion_timestamp").alias("ingestion_timestamp"),
        F.col("metadata.raw_hash").alias("review_id"),
        
        F.lit("None").alias("tmdb_id"),
        F.col("raw_payload.movie_id").alias("imdb_id"),
        F.col("raw_payload.author").alias("author"),
        F.col("raw_payload.rating").alias("rating"),
        F.col("raw_payload.content").alias("content"),
        F.col("raw_payload.date").alias("created_at")
    ).dropna(subset=["imdb_id", "author", "content", "created_at"])

    # Xử lý trùng lặp bằng window function
    window_spec = Window.partitionBy("review_id").orderBy(F.desc("ingestion_timestamp"))
    dedup_df = transform_df.withColumn("rn", F.row_number().over(window_spec)) \
                           .filter(F.col("rn") == 1) \
                           .drop("rn")
    
    validated_columns = [
        F.col(field.name).cast(field.dataType).alias(field.name)
        for field in Review_Schema.fields
    ]
    final_silver_df = dedup_df.select(*validated_columns)
    write_data_to_minio(final_silver_df, "silver", "reviews/imdb", target_date)    
    spark.stop()