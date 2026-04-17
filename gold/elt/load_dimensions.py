import sys
import os
from datetime import datetime
from pyspark.sql import functions as F
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from transformation.common.spark_utils import get_spark_session, get_layer_path
from gold.db_utils import write_to_postgres, get_jdbc_url, get_postgres_properties

bucket_name = "silver"

def get_spark():
    spark = get_spark_session("Gold_Load_Dimensions")
    return spark

def get_reviews(spark, target_date=None):
    reviews_path = get_layer_path("s3a://", bucket_name, "nlp/reviews_enriched", target_date)
    try:
        df = spark.read.parquet(reviews_path + "*.parquet")
        return df
    except Exception as e:
        print(f"[!] Mục {reviews_path} không có dữ liệu (có thể do Ingestion skip dedup hoặc ngày này không có dữ liệu).")
        return None

def get_movies(spark, base_prefix, target_date=None):
    movie_path = get_layer_path("s3a://", bucket_name, base_prefix, target_date)
    try:
        movie_df = spark.read.parquet(movie_path + "*.parquet")
        return movie_df
    except Exception as e:
        print(f"[!] Mục {movie_path} không tồn tại hoặc không có dữ liệu mới.")
        return None

def load_movie(spark, target_date):
    imdb_movies = get_movies(spark, "imdb/movies", target_date)
    tmdb_movies = get_movies(spark, "tmdb/movies", target_date)

    if imdb_movies is None and tmdb_movies is None:
        return None

    if imdb_movies is not None:
        imdb_movies_metadata = imdb_movies.select(
            F.col("imdb_id").alias("imdb_id"),
            F.col("title").alias("imdb_title"),
            F.col("description").alias("overview"),
            F.col("release_date"),
            F.col("duration_seconds"),
            F.concat_ws(", ", F.col("genres")).alias("genres"),
            F.col("imdb_rating")
        ).dropDuplicates(["imdb_id"])
    
    if tmdb_movies is not None:
        tmdb_movies_metadata = tmdb_movies.select(
            F.col("tmdb_id"),
            F.col("imdb_id"),
            F.col("title").alias("tmdb_title"),
            F.col("original_language"),
            F.col("tmdb_vote_average").alias("tmdb_rating")
        ).filter(F.col("imdb_id").isNotNull()).dropDuplicates(["imdb_id"])

    if imdb_movies is not None and tmdb_movies is not None:
        merged_movies = imdb_movies_metadata.join(tmdb_movies_metadata, on="imdb_id", how="full_outer")
    elif imdb_movies is not None:
        merged_movies = imdb_movies_metadata \
            .withColumn("tmdb_id", F.lit(None).cast("string")) \
            .withColumn("tmdb_title", F.lit(None).cast("string")) \
            .withColumn("original_language", F.lit(None).cast("string")) \
            .withColumn("tmdb_rating", F.lit(0.0).cast("double"))
    else:
        merged_movies = tmdb_movies_metadata \
            .withColumn("imdb_title", F.lit(None).cast("string")) \
            .withColumn("overview", F.lit(None).cast("string")) \
            .withColumn("release_date", F.lit(None).cast("string")) \
            .withColumn("duration_seconds", F.lit(None).cast("bigint")) \
            .withColumn("genres", F.lit(None).cast("string")) \
            .withColumn("imdb_rating", F.lit(0.0).cast("double"))

    final_movies_df = merged_movies.withColumn(
        "title", F.coalesce(F.col("imdb_title"), F.col("tmdb_title"))
    ).drop("imdb_title", "tmdb_title").na.fill({
        "tmdb_rating": 0.0, 
        "imdb_rating": 0.0
    })
    return final_movies_df

def load_dimensions(target_date):
    spark = get_spark()
    reviews_df = get_reviews(spark, target_date)
    
    if reviews_df is not None:
        print("\n[+] Đang xử lý bảng dim_review_text...")
        dim_reviews_text = reviews_df.select(
            "review_id",
            F.col("content").alias("review_text"),
            F.length(F.col("content")).alias("review_text_length")
        ).dropDuplicates(["review_id"])
        write_to_postgres(dim_reviews_text, "dim_reviews_text", mode="append")

        print("\n[+] Đang xử lý bảng dim_date...")
        dim_date = reviews_df.select(
            F.date_format("created_at", "yyyyMMdd").alias("date_id"),
            F.to_date("created_at").alias("full_date"),
            F.year("created_at").alias("year"),
            F.month("created_at").alias("month"),
            F.dayofmonth("created_at").alias("day"),
            F.quarter("created_at").alias("quarter"),
            F.dayofweek("created_at").alias("day_of_week")
        ).filter(F.col("date_id").isNotNull()).dropDuplicates(["date_id"])
        write_to_postgres(dim_date, "dim_date", mode="append")
    else:
        print("[!] Bỏ qua load dim_reviews_text và dim_date vì không có dữ liệu review mới.")

    dim_movie = load_movie(spark, target_date)
    if dim_movie is not None:
        write_to_postgres(dim_movie, "dim_movie", mode="append")
    else:
        print("[!] Bỏ qua load dim_movie vì không có dữ liệu phim mới.")
        
    spark.stop()
    
if __name__ == "__main__":
    target_date = datetime.now().strftime("%Y-%m-%d")
    load_dimensions(target_date)