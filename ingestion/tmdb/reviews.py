import os
import sys
from dotenv import load_dotenv
import requests
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from ingestion.common.upload_data import upload_to_minio

load_dotenv()
TMDB_API_KEY = os.getenv("TMDB_API_KEY")

def get_imdb_id(movie_id):
    url = f"https://api.themoviedb.org/3/movie/{movie_id}"
    params = {"api_key": TMDB_API_KEY}
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    return data.get("imdb_id")

def ingest_tmdb_reviews(movie_id, max_reviews):
    page = 1
    all_reviews = []
    try:
        imdb_id = get_imdb_id(movie_id)
        while len(all_reviews) < max_reviews:
            url = f"https://api.themoviedb.org/3/movie/{movie_id}/reviews"
            params = {
                "api_key": TMDB_API_KEY,
                "page": page
            }
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                results = data.get("results", [])
                if not results: break
                for reviews in results:
                    reviews["movie_id"] = movie_id
                    reviews["imdb_id"] = imdb_id
                    all_reviews.append(reviews)
                if page >= data.get("total_pages", 0): break
            page += 1
        if all_reviews:
            upload_to_minio(
                raw_data=all_reviews,
                source="tmdb",
                entity="reviews",
                methods="API Calling",
                http_status=200,
                search_params={"movie_id": movie_id}
            )
            return f"Reviews TMDB OK: {movie_id} ({len(all_reviews)})"
        return f"Reviews TMDB: {movie_id} (No data)"
    except Exception as e:
        return f"Reviews TMDB Error {movie_id}: {e}"