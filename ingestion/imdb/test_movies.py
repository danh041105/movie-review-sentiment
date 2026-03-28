import requests
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from ingestion.common.upload_data import upload_to_minio

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
        response.raise_for_status()
        data = response.json()
        edges = data.get("data", {}).get("chartTitles", {}).get("edges", [])
        movie_ids = [edge["node"]["id"] for edge in edges]
        return movie_ids
    except Exception as e:
        print(f"Lỗi: không thể kết nối đến với GraphQL {e}")
        return []

def ingest_all_movies(movie_ids):
    """
    Giai đoạn 1: Cào toàn bộ Metadata hàng loạt và đẩy lên MinIO.
    Sử dụng cơ chế Bulk để tối ưu hóa tốc độ (giảm từ 100 request xuống còn 1).
    """
    limit = len(movie_ids)
    print(f"🚀 Đang bắt đầu Bulk Ingest cho {limit} phim...")
    
    # Sử dụng MOVIE_DETAIL_QUERY đã định nghĩa sẵn trong file của bạn
    payload = {
        "query": MOVIE_DETAIL_QUERY,
        "variables": {"first": limit}
    }

    try:
        response = requests.post(GRAPHQL_URL, headers=HEADERS, json=payload, timeout=20)
        response.raise_for_status()
        data = response.json()
        edges = data.get("data", {}).get("chartTitles", {}).get("edges", [])
        bulk_metadata = []
        for edge in edges:
            node = edge.get("node", {})
            if node:
                movie_data = {
                    'imdb_id': node.get('id'),
                    'title': node.get('titleText', {}).get('text'),
                    'description': node.get('plot', {}).get('plotText', {}).get('plainText'),
                    'release_date': node.get('releaseDate'),
                    'duration_seconds': node.get('runtime', {}).get('seconds'),
                    'genres': [g.get('text') for g in node.get('genres', {}).get('genres', [])] if node.get('genres') else [],
                    'rating': node.get('ratingsSummary', {}).get('aggregateRating'),
                    'vote_count': node.get('ratingsSummary', {}).get('voteCount')
                }
                bulk_metadata.append(movie_data)
        if bulk_metadata:
            upload_to_minio(
                raw_data=bulk_metadata,
                source="imdb",
                entity="movies",
                methods="Bulk_Scraping",
                http_status=200,
                search_params={"movie_count": len(bulk_metadata)}
            )
            print(f"Thành công: Đã đẩy {len(bulk_metadata)} phim lên MinIO server .125")
            return True
        
        return bulk_metadata

    except Exception as e:
        print(f"Lỗi trong quá trình Ingest: {e}")
        return []
