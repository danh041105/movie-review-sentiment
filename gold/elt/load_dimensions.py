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
    df = spark.read.parquet(reviews_path + "*.parquet")
    return df

def get_movies(spark, base_prefix, target_date=None):
    movie_path = get_layer_path("s3a://", bucket_name, base_prefix, target_date)
    movie_df = spark.read.parquet(movie_path + "*.parquet")
    return movie_df

def load_movie(spark, target_date):
    imdb_movies = get_movies(spark, "imdb/movies", target_date)
    tmdb_movies = get_movies(spark, "tmdb/movies", target_date)

    imdb_movies_metadata = imdb_movies.select(
        F.col("imdb_id").alias("imdb_id"),
        F.col("title").alias("imdb_title"),
        F.col("description").alias("overview"),
        F.col("release_date"),
        F.col("duration_seconds"),
        F.concat_ws(", ", F.col("genres")).alias("genres"),
        F.col("imdb_rating")
    ).dropDuplicates(["imdb_id"])
    
    tmdb_movies_metadata = tmdb_movies.select(
        F.col("tmdb_id"),
        F.col("imdb_id"),
        F.col("title").alias("tmdb_title"),
        F.col("original_language"),
        F.col("tmdb_vote_average").alias("tmdb_rating")
    ).filter(F.col("imdb_id").isNotNull()).dropDuplicates(["imdb_id"])

    merged_movies = imdb_movies_metadata.join(tmdb_movies_metadata, on="imdb_id", how="full_outer")

    final_movies_df = merged_movies.withColumn(
        "title", F.coalesce(F.col("imdb_title"), F.col("tmdb_title"))
    ).drop("imdb_title", "tmdb_title").na.fill({  # <--- THÊM "tmdb_title" VÀO LỆNH DROP
        "tmdb_rating": 0.0, 
        "imdb_rating": 0.0
    })
    return final_movies_df

def load_dimensions(target_date):
    spark = get_spark()
    reviews_df = get_reviews(spark, target_date)
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

    dim_movie = load_movie(spark, target_date)
    write_to_postgres(dim_movie, "dim_movie", mode="append")
    spark.stop()
    
if __name__ == "__main__":
    target_date = datetime.now().strftime("%Y-%m-%d")
    load_dimensions(target_date)