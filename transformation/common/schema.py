from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType,
    ArrayType, DateType, TimestampType,
    IntegerType
)
TMDB_Movie_Schema = StructType([
    # --- Tracking / Lineage ---
    StructField("ingestion_id",         StringType(),   nullable=False),  # UUID từ metadata
    StructField("source_system",        StringType(),   nullable=False),  # "imdb" | "tmdb" | "merged"
    StructField("ingestion_timestamp",  TimestampType(), nullable=True),
    StructField("raw_hash",             StringType(),   nullable=True),   # Dùng để detect thay đổi
    # --- Identity ---
    StructField("tmdb_id",              StringType(),   nullable=False),
    StructField("imdb_id",              StringType(),   nullable=False),  # "tt0068646" — Join key chính
    StructField("title",                StringType(),   nullable=True),
    StructField("original_title",       StringType(),   nullable=True),   # TMDB only
    # --- Classification ---
    StructField("genres",               ArrayType(StringType()), nullable=True),
    StructField("original_language",    StringType(),   nullable=True),   # TMDB only
    StructField("release_date",         DateType(),     nullable=True),   # Chuẩn hóa từ {day,month,year} của IMDb
    StructField("duration_seconds",     LongType(),     nullable=True),   # Chuẩn hóa: TMDB runtime (phút) * 60
    # --- Ratings (giữ riêng vì khác nguồn, thang điểm khác nhau) ---
    StructField("tmdb_vote_average",    DoubleType(),   nullable=True),   # TMDB: 0.0 - 10.0
    StructField("tmdb_vote_count",      LongType(),     nullable=True),
        # --- Text fields (giữ cả 2 vì dùng cho NLP context) ---
    StructField("overview",             StringType(),   nullable=True)   # TMDB: dài hơn, tốt cho NLP
])
IMDB_Movie_Schema = StructType([
    StructField("ingestion_id",         StringType(),   nullable=False),  # UUID từ metadata
    StructField("source_system",        StringType(),   nullable=False),  # "imdb" | "tmdb" | "merged"
    StructField("ingestion_timestamp",  TimestampType(), nullable=True),
    StructField("raw_hash",             StringType(),   nullable=True),   # Dùng để detect thay đổi
    # --- Identity ---
    StructField("imdb_id",              StringType(),   nullable=False),  # "tt0068646" — Join key chính
    StructField("title",                StringType(),   nullable=True),
    # --- Classification ---
    StructField("genres",               ArrayType(StringType()), nullable=True),
    StructField("release_date",         DateType(),     nullable=True),   # Chuẩn hóa từ {day,month,year} của IMDb
    StructField("duration_seconds",     LongType(),     nullable=True),   # Chuẩn hóa: TMDB runtime (phút) * 60
    # --- Ratings (giữ riêng vì khác nguồn, thang điểm khác nhau) ---
    StructField("imdb_rating",          DoubleType(),   nullable=True),   # IMDb: 0.0 - 10.0
    StructField("imdb_vote_count",      LongType(),     nullable=True),
        # --- Text fields (giữ cả 2 vì dùng cho NLP context) ---
    StructField("description",          StringType(),   nullable=True),   # IMDb: ngắn, súc tích
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
    # --- Content (field quan trọng nhất cho NLP/ABSA) ---
    StructField("content",              StringType(),   nullable=False),  # Raw review text
    # --- Timeline ---
    StructField("created_at",           TimestampType(), nullable=True),
])