import requests
import json
import time
import sys
import random
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from ingestion.common.upload_data import upload_to_minio
from ingestion.common.redis_utils import is_reviews_changed, save_reviews_state

GRAPHQL_URL = "https://caching.graphql.imdb.com/"
PAGE_SIZE = 25
HEADERS = {
    "accept": "application/graphql+json, application/json",
    "accept-language": "en-US,en;q=0.9",
    "content-type": "application/json",
    "origin": "https://www.imdb.com",
    "referer": "https://www.imdb.com/",
    "user-agent": "Mozilla/5.0"
}
REVIEWS_QUERY = """
query TitleReviewsRefine($const: ID!, $filter: ReviewsFilter, $first: Int!, \
    $sort: ReviewsSort, $after: ID) {
    title(id: $const) {
        reviews(filter: $filter, first: $first, sort: $sort, after: $after) {
            edges {
                node {
                    author { nickName }
                    authorRating
                    submissionDate
                    summary { originalText }
                    text { originalText { plainText } }
                }
            }
            pageInfo{
                hasNextPage
                endCursor
            }
        }
    }
}
"""
def fetch_reviews(movie_id, max_reviews):
    all_reviews = []
    cursor = None
    has_next_page = True
    while has_next_page and len(all_reviews) < max_reviews:
        payload = {
            "operationName": "TitleReviewsRefine",
            "query": REVIEWS_QUERY,
            "variables": {
                "const": movie_id,
                "filter": {},
                "first": PAGE_SIZE,
                "sort": {
                    "by": "SUBMISSION_DATE",
                    "order": "DESC"
                },
                "after": cursor
            }
        }
        try:
            response = requests.post(GRAPHQL_URL,headers=HEADERS,json=payload)
            response.raise_for_status()
            data = response.json()
            if "errors" in data:
                print("GraphQL error:", data["errors"])
                break
            reviews_data = data.get("data", {}).get("title", {}).get("reviews", {})
            edges = reviews_data.get("edges", [])
            for edge in edges:
                node = edge.get("node", {})
                all_reviews.append({
                    "movie_id": movie_id,
                    "author": node.get("author", {}).get("nickName"),
                    "rating": node.get("authorRating"),
                    "date": node.get("submissionDate"),
                    "title": node.get("summary", {}).get("text"),
                    "content": node.get("text", {}).get("originalText", {}).get("plainText")
                })
            page_info = reviews_data.get("pageInfo", {})
            has_next_page = page_info.get("hasNextPage")
            cursor = page_info.get("endCursor")
            if len(all_reviews) >= max_reviews:
                break
            # Dùng random sleep (2-5s) thay vì fixed 1s để tránh HTTP 429 Too Many Requests
            time.sleep(random.uniform(2, 5))
        except Exception as e:
            print("Error:", e)
            break
    return all_reviews

def ingest_reviews_movie(movie_id, review_per_movie):
    reviews = fetch_reviews(movie_id, max_reviews=review_per_movie)
    if reviews:
        # ===== REDIS DEDUP: So sánh hash với lần cào trước =====
        if not is_reviews_changed("imdb", str(movie_id), reviews):
            # Reviews giống hệt → chỉ refresh TTL, không upload lại
            save_reviews_state("imdb", str(movie_id), reviews, review_count=len(reviews))
            return f"Reviews IMDB SKIP (không đổi): {movie_id} ({len(reviews)} reviews)"

        # Reviews mới hoặc đã thay đổi → upload lên MinIO
        upload_to_minio(
            raw_data=reviews,
            source="imdb",
            entity="reviews",
            methods="Scraping",
            http_status=200,
            search_params={"movie_id": movie_id, "count": len(reviews)}
        )
        save_reviews_state("imdb", str(movie_id), reviews, review_count=len(reviews))
        return f"Reviews IMDB OK (mới/cập nhật): {movie_id} ({len(reviews)} reviews)"
    return f"Reviews IMDB: {movie_id} (No data)"