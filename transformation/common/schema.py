from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType,
    ArrayType, DateType, TimestampType,
    IntegerType
)
TMDB_Movie_Schema = StructType([
    StructField("ingestion_id",         StringType(),   nullable=False),  # UUID từ metadata
    StructField("source_system",        StringType(),   nullable=False),  # "imdb" | "tmdb" | "merged"
    StructField("ingestion_timestamp",  TimestampType(), nullable=True),
    StructField("raw_hash",             StringType(),   nullable=True),   # Dùng để detect thay đổi

    StructField("tmdb_id",              StringType(),   nullable=False),
    StructField("imdb_id",              StringType(),   nullable=False),  # "tt0068646" — Join key chính
    StructField("title",                StringType(),   nullable=True),
    StructField("original_title",       StringType(),   nullable=True),   # TMDB only
    StructField("genres",               ArrayType(StringType()), nullable=True),
    StructField("original_language",    StringType(),   nullable=True),   # TMDB only
    StructField("release_date",         DateType(),     nullable=True),   # Chuẩn hóa từ {day,month,year} của IMDb
    StructField("duration_seconds",     LongType(),     nullable=True),   # Chuẩn hóa: TMDB runtime (phút) * 60
    StructField("tmdb_vote_average",    DoubleType(),   nullable=True),   # TMDB: 0.0 - 10.0
    StructField("tmdb_vote_count",      LongType(),     nullable=True),
    StructField("overview",             StringType(),   nullable=True),
    StructField("budget",               StringType(),   nullable=True),
    StructField("revenue",              StringType(),   nullable=True),
])
IMDB_Movie_Schema = StructType([
    StructField("ingestion_id",         StringType(),   nullable=False),  # UUID từ metadata
    StructField("source_system",        StringType(),   nullable=False),  # "imdb" | "tmdb" | "merged"
    StructField("ingestion_timestamp",  TimestampType(), nullable=True),
    StructField("raw_hash",             StringType(),   nullable=True),   # Dùng để detect thay đổi

    StructField("imdb_id",              StringType(),   nullable=False),  # "tt0068646" — Join key chính
    StructField("title",                StringType(),   nullable=True),
    StructField("genres",               ArrayType(StringType()), nullable=True),
    StructField("release_date",         DateType(),     nullable=True),   # Chuẩn hóa từ {day,month,year} của IMDb
    StructField("duration_seconds",     LongType(),     nullable=True),   # Chuẩn hóa: TMDB runtime (phút) * 60
    StructField("imdb_rating",          DoubleType(),   nullable=True),   # IMDb: 0.0 - 10.0
    StructField("imdb_vote_count",      LongType(),     nullable=True),
    StructField("description",          StringType(),   nullable=True),   # IMDb: ngắn, súc tích
    StructField("production_budget",    StringType(),   nullable=True),
    StructField("worldwide_gross",      StringType(),   nullable=True),
])
# =============================================================================
# Movies Schema — Hợp nhất các tên bộ phim
# =============================================================================
Movie_Schema = StructType([
    StructField("ingestion_id",         StringType(), nullable=False),  # UUID từ metadata
    StructField("source_system",        StringType(), nullable=False),  # "imdb" | "tmdb"
    StructField("ingestion_timestamp",  TimestampType(), nullable=True),
    
    StructField("imdb_id",              StringType(),   nullable=False),
    StructField("tmdb_id",              StringType(),   nullable=True),
    StructField("title",                StringType(),   nullable=True),
    StructField("original_title",       StringType(),   nullable=True),
    StructField("genres",               ArrayType(StringType()), nullable=True),
    StructField("original_language",    StringType(),   nullable=True),
    StructField("release_date",         DateType(),     nullable=True),
    StructField("duration_seconds",     LongType(),     nullable=True),
    StructField("tmdb_vote_average",    DoubleType(),   nullable=True),
    StructField("tmdb_vote_count",      LongType(),     nullable=True),
    StructField("imdb_rating",          DoubleType(),   nullable=True),
    StructField("imdb_vote_count",      LongType(),     nullable=True),
    StructField("overview",             StringType(),   nullable=True),
    StructField("description",          StringType(),   nullable=True),
    StructField("production_budget",    StringType(),   nullable=True),
    StructField("worldwide_gross",      StringType(),   nullable=True),
    StructField("budget",               StringType(),   nullable=True),
    StructField("revenue",              StringType(),   nullable=True),
])

# =============================================================================
# Unified schema — NLP chỉ cần content + movie_id, không quan tâm nguồn
# =============================================================================
Review_Schema = StructType([
    # --- Tracking / Lineage ---
    StructField("ingestion_id",         StringType(), nullable=False),  # UUID từ metadata
    StructField("source_system",        StringType(), nullable=False),  # "imdb" | "tmdb"
    StructField("ingestion_timestamp",  TimestampType(), nullable=True),
    StructField("review_id",             StringType(),   nullable=True),
    # --- Identity ---
    StructField("tmdb_id",              StringType(),   nullable=True),   # Có thể bên 
    StructField("imdb_id",              StringType(),   nullable=True), # Backfill ở Gold layer khi join với Movie_Schema 
    # --- Author ---
    StructField("author",               StringType(),   nullable=True),   # username
    StructField("rating",               DoubleType(),   nullable=True),   # author_details.rating — có thể null nếu không rate
    StructField("content",              StringType(),   nullable=False),
    StructField("created_at",           TimestampType(), nullable=True),
])