import requests
import json
from bs4 import BeautifulSoup
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from upload_data import upload_to_minio

GRAPHQL_URL = "https://caching.graphql.imdb.com/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/graphql-response+json",
    "Accept-Language": "en-US,en;q=0.9",
    "Content-Type": "application/json",
}
MOVIE_DETAIL_QUERY = """
query GetMovieDetail($id: ID!) {
  title(id: $id) {
    id
    titleText { text }
    plot { plotText { plainText } }
    primaryImage { url }
    releaseDate { day month year }
    runtime { seconds }
    ratingsSummary { aggregateRating voteCount }
    genres { genres { text } }
  }
}
"""

def get_json_pattern(url, id, type):
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    json_pattern = soup.find("script", id=id, type=type)
    return json_pattern

def fetch_movie_ids():
    url = "https://www.imdb.com/chart/moviemeter"
    json_pattern = get_json_pattern(url=url, id="__NEXT_DATA__", type="application/json")
    if not json_pattern:
        return []
    try: 
        data = json.loads(json_pattern.string)
        movie_data = data.get("props", {}).get("pageProps", {}).get("pageData", {}) \
                            .get("chartTitles", {}).get("edges", [])
        movie_id_list = []
        for movie in movie_data:
            node = movie.get("node", {})
            id = node.get("id")
            movie_id_list.append(id)
        return movie_id_list
    except:
        return []

def get_movie_info(movie_id):
    payload = {
        "query": MOVIE_DETAIL_QUERY,
        "variables": {"id": movie_id}
    }
    try:
        response = requests.post(GRAPHQL_URL, headers=HEADERS, json=payload, timeout=10)
        response.raise_for_status()

        result = response.json()
        movie = result.get("data", {}).get("title")
        if not movie:
            return None
        info = {
            'imdb_id': movie_id,
            'title': movie.get('titleText', {}).get('text'),
            'description': movie.get('plot', {}).get('plotText', {}).get('plainText'),
            'releaseDate': movie.get('releaseDate', {}),
            'duration_seconds': movie.get('runtime', {}).get('seconds'),
            'genres': [g.get('text') for g in movie.get('genres', {}).get('genres', [])],
            'rating': movie.get('ratingsSummary', {}).get('aggregateRating'),
            'vote_count': movie.get('ratingsSummary', {}).get('voteCount')
        }
        return info

    except Exception as e:
        print(f"Lỗi GraphQL cho phim {movie_id}: {e}")
        return None

def ingest_imdb_movies(movie_id):
    metadata = get_movie_info(movie_id)
    if metadata:
        upload_to_minio(
            raw_data=[metadata],
            source="imdb",
            entity="movies",
            methods="Web Scraping",
            http_status=200,
            search_params={"movie_id": movie_id}
        )
        return f"Metadata hoàn thành: {movie_id}"
    return f"Metadata thất bại: {movie_id}"

# if __name__ == "__main__":
#     data = fetch_movie_ids()
#     movie = data[0]
#     movie_info = get_movie_info(movie)
#     print(movie_info)