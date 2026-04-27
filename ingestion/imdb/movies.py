import requests
import sys
import os
import json
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from ingestion.common.upload_data import upload_to_minio
from ingestion.common.redis_utils import is_movie_changed, save_movie_state

GRAPHQL_URL = "https://caching.graphql.imdb.com/"
HEADERS = {
    "accept": "application/graphql+json, application/json",
    "accept-language": "en-US,en;q=0.9",
    "content-type": "application/json",
    "origin": "https://www.imdb.com",
    "referer": "https://www.imdb.com/",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
    "x-imdb-client-name": "imdb-web-next-localized",
    "x-imdb-user-country": "US",
    "x-imdb-user-language": "en-US",
}
POPULAR_MOVIE_QUERY = """
    query GetMovieIDs($first: Int!){
        chartTitles(first: $first, chart: {chartType: MOST_POPULAR_MOVIES}){
        edges{
            node{
                id
            }
        }
        }
    }
"""
MOVIE_DETAIL_QUERY = """
query GetPopularMovies($first: Int!) {
  chartTitles(first: $first, chart: {chartType: MOST_POPULAR_MOVIES}) {
    edges {
      node {
        id
        titleText { text }
        plot { plotText { plainText } }
        releaseDate { day month year }
        runtime { seconds }
        ratingsSummary { aggregateRating voteCount }
        genres { genres { text } }
        productionBudget{ budget { amount currency}}
        rankedLifetimeGross(boxOfficeArea: WORLDWIDE){ 
            total  {amount currency} 
        }
      }
    }
  }
}
"""
def fetch_movie_ids(limit=100):
    movie_id_payload={
        "query": POPULAR_MOVIE_QUERY,
        "variables": {"first": limit}
    }
    try:
        response = requests.post(GRAPHQL_URL, headers=HEADERS, json=movie_id_payload)
        if response.status_code == 200:
            data = response.json()
            edges = data.get("data", {}).get("chartTitles", {}).get("edges", [])
            movie_ids = [edge["node"]["id"] for edge in edges]
            return movie_ids
    except Exception as e:
        print(f"Lỗi: không thể kết nối đến với GraphQL {e}")
        return []

def get_money(node, key1, key2):
    top_level = node.get(key1)
    if not top_level:
        return None
    budget_data = top_level.get(key2)
    if not budget_data:
        return None
    amount = budget_data.get("amount")
    currency = budget_data.get("currency")
    if amount and currency:
        return f"{amount} {currency}"
    return None

def get_movies_detail(movie_ids):
    limit = len(movie_ids)
    print(f"Đang lấy thông tin cho {limit} phim từ IMDb...")
    payload = {
        "query": MOVIE_DETAIL_QUERY,
        "variables": {"first": limit}
    }
    try:
        response = requests.post(GRAPHQL_URL, headers=HEADERS, json=payload, timeout=20)
        response.raise_for_status()
        data = response.json()

        if not data or data.get("data") is None:
            error_msg = data.get("errors", "Không rõ nguyên nhân")
            print(f" Lỗi từ IMDb GraphQL: {error_msg}")
            return []

        edges = data["data"].get("chartTitles", {}).get("edges", [])
        bulk_metadata = []

        for edge in edges:
            node = edge.get("node")
            if not node: continue
            movie_data = {
                'imdb_id': node.get('id'),
                'title': (node.get('titleText') or {}).get('text'),
                'description': (node.get('plot', {}) or {}).get('plotText', {}).get('plainText'),
                'release_date': node.get('releaseDate'),
                'duration_seconds': (node.get('runtime') or {}).get('seconds'),
                'genres': [g.get('text') for g in (node.get('genres') or {}).get('genres', [])] if node.get('genres') else [],
                'rating': (node.get('ratingsSummary') or {}).get('aggregateRating'),
                'vote_count': (node.get('ratingsSummary') or {}).get('voteCount'),
                'production_budget': get_money(node, "productionBudget", "budget"),
                "worldwide_gross": get_money(node, "rankedLifetimeGross", "total")
            }
            bulk_metadata.append(movie_data)

        print(f"Đã lấy thành công {len(bulk_metadata)} phim từ IMDb.")
        return bulk_metadata
    except Exception as e:
        print(f"Lỗi khi lấy thông tin phim từ IMDb: {e}")
        return []


def upload_movies_to_minio(bulk_metadata):
    if not bulk_metadata:
        print("Không có dữ liệu phim để upload.")
        return 0

    # ===== REDIS DEDUP: Chỉ upload phim có dữ liệu thay đổi =====
    changed_movies = []
    skipped_count = 0

    for movie_data in bulk_metadata:
        movie_id = movie_data.get('imdb_id')
        if is_movie_changed("imdb", movie_id, movie_data):
            changed_movies.append(movie_data)
        else:
            skipped_count += 1

    print(f"[Dedup] {len(changed_movies)} phim mới/thay đổi | {skipped_count} phim giống hệt (skip)")

    # Chỉ upload những phim thực sự thay đổi
    if changed_movies:
        upload_to_minio(
            raw_data=changed_movies,
            source="imdb",
            entity="movies",
            methods="Bulk_Scraping",
            http_status=200,
            search_params={"movie_count": len(changed_movies)}
        )

    for movie_data in bulk_metadata:
        save_movie_state("imdb", movie_data.get('imdb_id'), movie_data)
    return len(changed_movies)

if __name__ == "__main__":
    movie_ids = fetch_movie_ids()
    movies_detail = get_movies_detail(movie_ids)
    upload_movies_to_minio(movies_detail)
    # movies_detail_json = json.dumps(movies_detail, indent=4)
    # print(movies_detail_json)
