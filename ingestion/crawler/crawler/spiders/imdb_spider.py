import scrapy
import json
import time
import hashlib
from crawler.items import Info
from crawler.items import MovieItem
import uuid
import datetime
class ImdbspiderSpider(scrapy.Spider):
    name = "imdb_spider"
    allowed_domains = ["imdb.com"]
    start_urls = ["https://www.imdb.com/chart/moviemeter/"]

    def __init__(self, *args, **kwargs):
        super(ImdbspiderSpider, self).__init__(*args, **kwargs)
        self.extraction_start_time = None
        self.movies_count = 0
        self.batch_items = []  # Batch items for bulk upload
        self.batch_size = 25   # Upload every 25 items
        
    def parse(self, response):
        self.extraction_start_time = time.time()
        raw_data = response.css("script[id='__NEXT_DATA__']::text").get()
        if not raw_data:
            self.logger.error(f"Không có data trong {response.url}")
            return
        json_data = json.loads(raw_data)
        try:
            needed_data = json_data['props']['pageProps']['pageData']['chartTitles']['edges']
        except (KeyError, TypeError) as e:
            self.logger.error(f"Sai định dạng schema{e}")
        
        for index, movie in enumerate(needed_data):
            movie_item = self._build_movie_item(
                        movie_data=movie,
                        source_url=response.url,
                        rank=index + 1
                    )
            if movie_item:
                self.movies_count += 1
                yield movie_item
        self.logger.info(f"Lấy ra thành công {len(needed_data)} phim từ {response.url}")

    def extract_raw_payload(self, movie_data):
        def get_nested(data, *keys, default=None):
            for key in keys:
                try:
                    data = data[key]
                except (KeyError, TypeError, IndexError):
                    return default
            return data
        raw_payload = {

            'title': get_nested(movie_data, 'node', 'titleText', 'text', default='N/A'),
            'imdb_id': get_nested(movie_data, 'node', 'id', default='N/A'),
            'currentRank': movie_data.get('currentRank', 'N/A'),
            'rankChange': movie_data.get('rankChange', {}).get('changeDirection', 'UNCHANGED'),
            'releaseYear': get_nested(movie_data, 'node', 'releaseYear', 'year', default=None),
            'runtime': {
                'seconds': get_nested(movie_data, 'node', 'runtime', 'seconds', default=0),
                'display': get_nested(movie_data, 'node', 'runtime', 'display', default='N/A')
            },
            'rating': {
                'aggregate': get_nested(movie_data, 'node', 'ratingsSummary', 'aggregateRating', default=0),
                'vote_count': get_nested(movie_data, 'node', 'ratingsSummary', 'voteCount', default=0)
            },
            'description': get_nested(movie_data, 'node', 'plot', 'plotText', 'plainText', default=''),
            'primaryImage': get_nested(movie_data, 'node', 'primaryImage', 'url', default=''),
            'genres': get_nested(movie_data, 'node', 'genres', 'genres', default=[]),
        }
        return raw_payload
    
    def _calculate_quality_score(self, raw_payload):
        score = 0
        max_score = 100
        if raw_payload.get('title') and raw_payload['title'] != 'N/A':
            score += 20
        if raw_payload.get('imdb_id') and raw_payload['imdb_id'] != 'N/A':
            score += 15
        if raw_payload.get('rating', {}).get('aggregate', 0) > 0:
            score += 20
        if raw_payload.get('rating', {}).get('vote_count', 0) > 0:
            score += 15
        if raw_payload.get('description') and len(raw_payload['description']) > 10:
            score += 15
        if raw_payload.get('releaseYear') and raw_payload['releaseYear'] > 1800:
            score += 10
        if raw_payload.get('runtime', {}).get('seconds', 0) > 0:
            score += 5
        return min(score, max_score)

    def build_movie_item(self, movie_data, source_url, rank):

        raw_payload = self._extract_raw_payload(movie_data)
        raw_hash = hashlib.sha256(json.dumps(raw_payload, sort_keys=True).encode()).hexdigest()
        quality_score = self._calculate_quality_score(raw_payload)
        extraction_duration = time.time() - self.extraction_start_time
        item = MovieItem()
        
        item["ingestion_id"] = str(uuid.uuid4())
        item['source_system'] = 'imdb'
        item['entity'] = 'movie'
        item['extraction_method'] = 'web_scraping'
        item['ingestion_timestamp'] = datetime.utcnow().isoformat() + 'Z'
        item['http_status'] = 200
        item['search_parameters'] = {'chart': self._get_chart_name(source_url)}
        item['raw_hash'] = raw_hash
        item['raw_payload'] = raw_payload
        item['data_quality_score'] = quality_score
        item['extraction_duration_seconds'] = extraction_duration
        item['source_url'] = source_url
    
        return item