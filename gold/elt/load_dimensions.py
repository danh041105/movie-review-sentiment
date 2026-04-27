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
    merged_movies_path = get_layer_path("s3a://", bucket_name, "merged_movies", target_date)
    try:
        merged_df = spark.read.parquet(merged_movies_path + "*.parquet")
    except Exception as e:
        print(f"[!] Không có dữ liệu merged_movies tại {merged_movies_path}")
        return None

    dim_movie = merged_df.select(
        F.col("imdb_id"),
        F.col("tmdb_id"),
        F.coalesce(F.col("title"), F.lit("Unknown")).alias("title"),
        F.col("original_title"),
        F.col("original_language"),
        F.col("release_date"),
        F.concat_ws(", ", F.col("genres")).alias("genres"),
        F.col("duration_seconds"),
        F.coalesce(F.col("overview"), F.col("description")).alias("overview"),
        F.col("tmdb_vote_average").alias("tmdb_rating"),
        F.col("tmdb_vote_count"),
        F.col("imdb_rating"),
        F.col("imdb_vote_count"),
        F.col("production_budget"),
        F.col("worldwide_gross"),
        F.col("budget").alias("tmdb_budget"),
        F.col("revenue").alias("tmdb_revenue"),
    ).dropDuplicates(["imdb_id"]).na.fill({
        "tmdb_rating": 0.0,
        "imdb_rating": 0.0
    })
    return dim_movie

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
        
        # Đọc review_id hiện có từ Postgres để tránh lỗi trùng lặp do ON CONFLICT
        existing_reviews = spark.read.jdbc(url=get_jdbc_url(), table="dim_reviews_text", properties=get_postgres_properties()).select("review_id")
        new_reviews = dim_reviews_text.join(existing_reviews, on="review_id", how="left_anti")
        
        write_to_postgres(new_reviews, "dim_reviews_text", mode="append")

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
        
        # Đọc date_id hiện có từ Postgres
        existing_dates = spark.read.jdbc(url=get_jdbc_url(), table="dim_date", properties=get_postgres_properties()).select("date_id")
        new_dates = dim_date.join(existing_dates, on="date_id", how="left_anti")
        
        write_to_postgres(new_dates, "dim_date", mode="append")
    else:
        print("[!] Bỏ qua load dim_reviews_text và dim_date vì không có dữ liệu review mới.")

    dim_movie = load_movie(spark, target_date)
    if dim_movie is not None:
        # Lấy data hiện có để tránh lỗi unique constraint imdb_id
        existing_movies = spark.read.jdbc(url=get_jdbc_url(), table="dim_movie", properties=get_postgres_properties()).select("imdb_id")
        new_movies = dim_movie.join(existing_movies, on="imdb_id", how="left_anti")
        
        write_to_postgres(new_movies, "dim_movie", mode="append")
    else:
        print("[!] Bỏ qua load dim_movie vì không có dữ liệu phim mới.")
        
    spark.stop()
    
if __name__ == "__main__":
    target_date = datetime.now().strftime("%Y-%m-%d")
    load_dimensions(target_date)