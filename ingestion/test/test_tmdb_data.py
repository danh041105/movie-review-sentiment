import os
import requests
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()
TMDB_TOKEN = os.getenv("TMDB_READ_ACCESS_TOKEN")
HEADERS = {
    "accept": "application/json",
    "Authorization": f"Bearer {TMDB_TOKEN}"
}
def get_genre_map():
    """Lấy danh sách ánh xạ ID -> Tên thể loại từ TMDB"""
    url = "https://api.themoviedb.org/3/genre/movie/list?language=en-US"
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        genres = response.json().get('genres', [])
        # Chuyển thành dictionary để tra cứu nhanh: {28: 'Action', 12: 'Adventure', ...}
        return {genre['id']: genre['name'] for genre in genres}
    except Exception as e:
        print(f"Error fetching genres: {e}")
        return {}

def get_monthly_trending_movies():
    today = datetime.now()
    last_month = today - timedelta(30)
    url = "https://api.themoviedb.org/3/discover/movie"
    params = {
        "language": "en-US",
        "sort_by": "popularity.desc",
        "primary_release_date.gte": last_month.strftime('%Y-%m-%d'),
        "primary_release_date.lte": today.strftime('%Y-%m-%d'),
        "page": 1
    }
    try:
        response = requests.get(url, headers=HEADERS, params=params)
        response.raise_for_status()
        data = response.json()
        movies = data.get('results', [])
        
        print("\n--- DANH SÁCH PHIM THỊNH HÀNH TRONG THÁNG\n")
        print("-" * 65)

        for idx, movie in enumerate(movies[:15]): # Hiển thị top 15
            for key, value in movie.items():
                print(f"{key}: {value}")
            print("-" * 20)

    except requests.exceptions.RequestException as e:
        print(f"Lỗi kết nối API: {e}")

if __name__ == "__main__":
    if not TMDB_TOKEN:
        print("Lỗi: Chưa tìm thấy TMDB_READ_TOKEN trong file .env")
    else:
        get_monthly_trending_movies()
