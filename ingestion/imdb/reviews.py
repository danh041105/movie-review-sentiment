import requests
import json
import time
import sys
import random
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from ingestion.common.upload_data import upload_to_minio
from ingestion.common.redis_utils import get_review_checkpoint, save_review_checkpoint

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

def fetch_reviews(movie_id, max_reviews, checkpoint_date=None):
    """
    Cào reviews từ IMDb GraphQL API với cơ chế Checkpoint (Early Stop).
    
    Args:
        movie_id: ID phim trên IMDb (vd: "tt1234567")
        max_reviews: Số lượng review tối đa cần cào
        checkpoint_date: Ngày checkpoint từ Redis (vd: "2026-04-27"). 
                         Nếu gặp review có ngày <= checkpoint → DỪNG NGAY.
    
    Returns:
        list: Danh sách chỉ chứa các review MỚI hơn checkpoint.
    """
    all_reviews = []
    cursor = None
    has_next_page = True
    hit_checkpoint = False

    while has_next_page and len(all_reviews) < max_reviews and not hit_checkpoint:
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
            response = requests.post(GRAPHQL_URL, headers=HEADERS, json=payload)
            response.raise_for_status()
            data = response.json()
            if "errors" in data:
                print("GraphQL error:", data["errors"])
                break
            reviews_data = data.get("data", {}).get("title", {}).get("reviews", {})
            edges = reviews_data.get("edges", [])
            for edge in edges:
                node = edge.get("node", {})
                review_date = node.get("submissionDate")
                
                # === CHECKPOINT: Dừng sớm nếu gặp review CŨ HƠN mốc ===
                # Luôn cào lại review của ngày checkpoint (tránh sót bài đăng muộn)
                if checkpoint_date and review_date and review_date < checkpoint_date:
                    hit_checkpoint = True
                    print(f"[Checkpoint] Gặp review ngày {review_date} <= checkpoint {checkpoint_date} → Dừng cào.")
                    break
                
                all_reviews.append({
                    "movie_id": movie_id,
                    "author": node.get("author", {}).get("nickName"),
                    "rating": node.get("authorRating"),
                    "date": review_date,
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
    """
    Hàm chính để cào reviews IMDb với cơ chế Checkpoint Incremental.
    
    Flow:
      1. Đọc checkpoint từ Redis (ngày review mới nhất của lần cào trước)
      2. Cào reviews từ API, dừng sớm khi gặp review cũ hơn checkpoint
      3. Nếu có review mới → Upload lên MinIO + Cập nhật checkpoint
      4. Nếu không có review mới → Skip hoàn toàn
    """
    # Bước 1: Đọc checkpoint từ Redis
    checkpoint_date = get_review_checkpoint("imdb", str(movie_id))
    if checkpoint_date:
        print(f"[Checkpoint] IMDb {movie_id}: Chỉ cào reviews sau ngày {checkpoint_date}")
    else:
        print(f"[Checkpoint] IMDb {movie_id}: Lần đầu cào → Full scrape")

    # Bước 2: Cào reviews (có early stop nếu có checkpoint)
    new_reviews = fetch_reviews(movie_id, max_reviews=review_per_movie, checkpoint_date=checkpoint_date)
    
    if not new_reviews:
        return f"Reviews IMDB SKIP (không có review mới): {movie_id}"
    
    # Bước 3: Upload CHỈ những review mới lên MinIO
    upload_to_minio(
        raw_data=new_reviews,
        source="imdb",
        entity="reviews",
        methods="Scraping",
        http_status=200,
        search_params={"movie_id": movie_id, "count": len(new_reviews)}
    )
    
    # Bước 4: Cập nhật checkpoint = ngày của review mới nhất (phần tử đầu tiên vì sort DESC)
    latest_date = new_reviews[0].get("date")
    if latest_date:
        save_review_checkpoint("imdb", str(movie_id), latest_date, review_count=len(new_reviews))
    
    return f"Reviews IMDB OK (incremental): {movie_id} ({len(new_reviews)} reviews mới)"