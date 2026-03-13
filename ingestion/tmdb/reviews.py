import os
import sys
from dotenv import load_dotenv
import requests
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from ingestion.common.upload_data import upload_to_minio

load_dotenv()
TMDB_API_KEY = os.getenv("TMDB_API_KEY")
def ingest_tmdb_reviews(movie_id, max_pages=2):
    try:
        all_reviews = []
        for page in range(1, max_pages + 1):
            url = f"https://api.themoviedb.org/3/movie/{movie_id}/reviews"
            params = {"api_key": TMDB_API_KEY , "page": page}

            response = requests.get(url, params=params)
            response.raise_for_status()

            data = response.json()
            results = data.get("results", [])
            if not results: break
            for r in results:
                r["movie_id"] = movie_id
                all_reviews.append(r)
            if page >= data.get("total_pages", 0): break

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