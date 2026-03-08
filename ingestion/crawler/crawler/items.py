# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
class MovieItem(scrapy.Item):
    ingestion_id = scrapy.Field()           # UUID for this ingestion record
    source_system = scrapy.Field()          # Source: 'imdb', 'tmdb', 'letterboxd'
    entity = scrapy.Field()                 # Entity type: 'movie'
    extraction_method = scrapy.Field()      # How data was extracted: 'web_scraping', 'api'
    ingestion_timestamp = scrapy.Field()    # ISO 8601 timestamp when data was ingested
    http_status = scrapy.Field()            # HTTP status code from source
    search_parameters = scrapy.Field()      # Query parameters used for extraction (dict)
    raw_hash = scrapy.Field()               # SHA256 hash of raw_payload for deduplication
    raw_payload = scrapy.Field()            # Complete raw data structure (dict)
    
    data_quality_score = scrapy.Field()     # 0-100 score based on field completeness
    extraction_duration_seconds = scrapy.Field()  # Time taken to extract this record
    source_url = scrapy.Field()             # URL from which data was extracted

class ReviewItem(scrapy.Item):
    ingestion_id = scrapy.Field()           # UUID for this ingestion record
    source_system = scrapy.Field()          # Source: 'imdb', 'tmdb', 'letterboxd'
    entity = scrapy.Field()                 # Entity type: 'review' or 'comment'
    extraction_method = scrapy.Field()      # How data was extracted
    ingestion_timestamp = scrapy.Field()    # ISO 8601 timestamp
    http_status = scrapy.Field()            # HTTP status code
    search_parameters = scrapy.Field()      # Query parameters (dict)
    raw_hash = scrapy.Field()               # SHA256 hash for deduplication
    raw_payload = scrapy.Field()            # Complete raw data structure (dict)
    
    parent_movie_id = scrapy.Field()        # Reference to movie being reviewed (IMDb ID, TMDB ID, etc.)
    data_quality_score = scrapy.Field()
    extraction_duration_seconds = scrapy.Field()
    source_url = scrapy.Field()