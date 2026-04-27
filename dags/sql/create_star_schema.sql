CREATE TABLE IF NOT EXISTS dim_source (
    source_id INT PRIMARY KEY,
    source_name VARCHAR(50) NOT NULL
);
INSERT INTO dim_source (source_id, source_name) VALUES (1, 'imdb'), (2, 'tmdb') ON CONFLICT DO NOTHING;

-- Bảng Cảm xúc (Có sẵn dữ liệu tĩnh)
CREATE TABLE IF NOT EXISTS dim_sentiment (
    sentiment_id INT PRIMARY KEY,
    sentiment_label VARCHAR(50) NOT NULL
);
INSERT INTO dim_sentiment (sentiment_id, sentiment_label) VALUES (1, 'Tích cực'), (0, 'Tiêu cực') ON CONFLICT DO NOTHING;

-- Bảng Thời gian
CREATE TABLE IF NOT EXISTS dim_date (
    date_id VARCHAR(8) PRIMARY KEY, -- Định dạng YYYYMMDD
    full_date DATE NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    quarter INT NOT NULL,
    day_of_week INT NOT NULL
);

-- Bảng Văn bản Review
CREATE TABLE IF NOT EXISTS dim_reviews_text (
    review_id VARCHAR(100) PRIMARY KEY,
    review_text TEXT NOT NULL,
    review_text_length INT
);

-- Bảng Phim (Áp dụng SCD Type 1) — Dựa trên merged_movies Silver
CREATE TABLE IF NOT EXISTS dim_movie (
    movie_id SERIAL PRIMARY KEY,
    imdb_id VARCHAR(50) UNIQUE,
    tmdb_id VARCHAR(50),
    title VARCHAR(255) NOT NULL,
    original_title VARCHAR(255),
    original_language VARCHAR(10),
    release_date DATE,
    genres VARCHAR(500),
    duration_seconds BIGINT,
    overview TEXT,
    tmdb_rating FLOAT DEFAULT 0.0,
    tmdb_vote_count BIGINT,
    imdb_rating FLOAT DEFAULT 0.0,
    imdb_vote_count BIGINT,
    production_budget VARCHAR(100),
    worldwide_gross VARCHAR(100),
    tmdb_budget VARCHAR(100),
    tmdb_revenue VARCHAR(100)
);

-- ==============================================================================
-- 3. TẠO BẢNG SỰ KIỆN (FACT TABLE)
-- ==============================================================================

CREATE TABLE IF NOT EXISTS fact_reviews (
    fact_id BIGSERIAL PRIMARY KEY,
    review_id VARCHAR(255) REFERENCES dim_reviews_text(review_id),
    movie_id INT REFERENCES dim_movie(movie_id),
    date_id VARCHAR(8) REFERENCES dim_date(date_id),
    source_id INT REFERENCES dim_source(source_id),
    sentiment_id INT REFERENCES dim_sentiment(sentiment_id),
    rating FLOAT,
    sentiment_confidence FLOAT,
    created_date TIMESTAMP,
    ingestion_id VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_fact_movie ON fact_reviews(movie_id);
CREATE INDEX IF NOT EXISTS idx_fact_date ON fact_reviews(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_sentiment ON fact_reviews(sentiment_id);