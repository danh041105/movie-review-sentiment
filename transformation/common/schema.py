from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType,
    ArrayType, DateType, TimestampType,
    IntegerType
)
Movie_Schema = StructType([
    # --- Tracking / Lineage ---
    StructField("ingestion_id",         StringType(),   nullable=False),  # UUID từ metadata
    StructField("source_system",        StringType(),   nullable=False),  # "imdb" | "tmdb" | "merged"
    StructField("ingestion_timestamp",  TimestampType(), nullable=True),
    StructField("raw_hash",             StringType(),   nullable=True),   # Dùng để detect thay đổi
    # --- Identity ---
    StructField("imdb_id",              StringType(),   nullable=False),  # "tt0068646" — Join key chính
    StructField("title",                StringType(),   nullable=True),
    StructField("original_title",       StringType(),   nullable=True),   # TMDB only
    # --- Classification ---
    StructField("genres",               ArrayType(StringType()), nullable=True),
    StructField("original_language",    StringType(),   nullable=True),   # TMDB only
    StructField("origin_country",       StringType(),   nullable=True),   # TMDB only
    StructField("production_companies", StringType(),   nullable=True),   # TMDB only
    StructField("release_date",         DateType(),     nullable=True),   # Chuẩn hóa từ {day,month,year} của IMDb
    StructField("duration_seconds",     LongType(),     nullable=True),   # Chuẩn hóa: TMDB runtime (phút) * 60
    # --- Ratings (giữ riêng vì khác nguồn, thang điểm khác nhau) ---
    StructField("imdb_rating",          DoubleType(),   nullable=True),   # IMDb: 0.0 - 10.0
    StructField("imdb_vote_count",      LongType(),     nullable=True),
    StructField("tmdb_vote_average",    DoubleType(),   nullable=True),   # TMDB: 0.0 - 10.0
    StructField("tmdb_vote_count",      LongType(),     nullable=True),
        # --- Text fields (giữ cả 2 vì dùng cho NLP context) ---
    StructField("description",          StringType(),   nullable=True),   # IMDb: ngắn, súc tích
    StructField("overview",             StringType(),   nullable=True)   # TMDB: dài hơn, tốt cho NLP
])

# =============================================================================
# Unified schema — NLP chỉ cần content + movie_id, không quan tâm nguồn
# =============================================================================
Review_Schema = StructType([
    # --- Tracking / Lineage ---
    StructField("ingestion_id", StringType(), nullable=False),  # UUID từ metadata
    StructField("source_system", StringType(), nullable=False),  # "imdb" | "tmdb"
    StructField("ingestion_timestamp",  TimestampType(), nullable=True),
    # --- Identity ---
    StructField("review_id",            StringType(),   nullable=False),  # ID gốc từ source
    StructField("movie_id",             StringType(),   nullable=False),  # TMDB movie_id → link về Movie_Schema qua imdb_id ở Gold
    StructField("imdb_id",              StringType(),   nullable=True),   # Backfill ở Gold layer khi join với Movie_Schema
    # --- Author ---
    StructField("author",               StringType(),   nullable=True),   # username
    StructField("author_name",          StringType(),   nullable=True),   # display name (TMDB có cả 2)
    StructField("rating",               DoubleType(),   nullable=True),   # author_details.rating — có thể null nếu không rate
    # --- Content (field quan trọng nhất cho NLP/ABSA) ---
    StructField("content",              StringType(),   nullable=False),  # Raw review text
    StructField("content_length",       IntegerType(),  nullable=True),   # len(content) — dùng để filter
    # --- Timeline ---
    StructField("created_at",           TimestampType(), nullable=True),
    StructField("updated_at",           TimestampType(), nullable=True),
    # --- Reference ---
    StructField("review_url",           StringType(),   nullable=True)
    # --- Quality flags (thêm ở Silver, dùng để filter trước khi đưa vào NLP) ---
])