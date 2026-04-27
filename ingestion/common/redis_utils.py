import os
import json
import hashlib
import redis
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_INGESTION_DB", "1"))  # DB 0 = Celery, DB 1 = Ingestion
DEFAULT_TTL_DAYS = 30

_redis_client = None

def get_redis_client():
    """
    Tạo kết nối Redis (Singleton).
    Nếu không kết nối được → trả về None, pipeline vẫn chạy bình thường.
    """
    global _redis_client
    if _redis_client is None:
        try:
            _redis_client = redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                db=REDIS_DB,
                decode_responses=True,
                socket_connect_timeout=5
            )
            _redis_client.ping()
            print(f"[Redis] Kết nối thành công tới {REDIS_HOST}:{REDIS_PORT} (DB={REDIS_DB})")
        except Exception as e:
            print(f"[Redis] Không thể kết nối: {e}")
            print(f"[Redis] Pipeline sẽ chạy bình thường KHÔNG có dedup.")
            _redis_client = None
    return _redis_client


def compute_data_hash(data):
    """Tính SHA256 hash của dữ liệu để so sánh thay đổi."""
    return hashlib.sha256(
        json.dumps(data, sort_keys=True, default=str).encode()
    ).hexdigest()


def _get_key(source, entity, item_id):
    """Tạo Redis key theo convention: ingestion:{source}:{entity}:{id}"""
    return f"ingestion:{source}:{entity}:{item_id}"


def is_movie_changed(source, movie_id, new_data):
    """
    So sánh hash của dữ liệu mới với hash đã lưu trong Redis.

    Returns:
        True  → Dữ liệu MỚI hoặc ĐÃ THAY ĐỔI  → CẦN upload lên MinIO
        False → Dữ liệu GIỐNG HỆT lần trước     → SKIP upload

    Nếu Redis không khả dụng → luôn trả True (fallback: cào bình thường)
    """
    client = get_redis_client()
    if client is None:
        return True  # Fallback: không có Redis thì cào như cũ

    key = _get_key(source, "movies", movie_id)
    new_hash = compute_data_hash(new_data)

    try:
        old_hash = client.hget(key, "metadata_hash")
        if old_hash is None:
            return True  # Phim mới, chưa từng cào
        return new_hash != old_hash  # True nếu hash khác = dữ liệu đã đổi
    except Exception as e:
        print(f"[Redis] Lỗi khi check movie {movie_id}: {e}")
        return True  # Lỗi thì cứ cào cho an toàn


def save_movie_state(source, movie_id, data, ttl_days=DEFAULT_TTL_DAYS):
    """
    Lưu trạng thái movie vào Redis sau khi xử lý xong.
    Bao gồm: metadata_hash + thời gian cào cuối.
    TTL mặc định 30 ngày — hết hạn sẽ tự xóa → lần sau cào lại từ đầu.
    """
    client = get_redis_client()
    if client is None:
        return

    key = _get_key(source, "movies", movie_id)
    state = {
        "metadata_hash": compute_data_hash(data),
        "last_scraped": datetime.now().isoformat(),
        "source": source,
        "movie_id": str(movie_id)
    }

    try:
        client.hset(key, mapping=state)
        client.expire(key, ttl_days * 86400)  # TTL tính bằng giây
    except Exception as e:
        print(f"[Redis] Lỗi khi lưu state movie {movie_id}: {e}")


# ==================== REVIEW DEDUP ====================

def is_reviews_changed(source, movie_id, reviews_data):
    """
    So sánh hash của danh sách reviews mới với hash đã lưu trong Redis.

    Reviews được hash theo từng movie_id (toàn bộ batch reviews của 1 phim).

    Returns:
        True  → Reviews MỚI hoặc ĐÃ THAY ĐỔI  → CẦN upload lên MinIO
        False → Reviews GIỐNG HỆT lần trước     → SKIP upload

    Nếu Redis không khả dụng → luôn trả True (fallback: cào bình thường)
    """
    client = get_redis_client()
    if client is None:
        return True

    key = _get_key(source, "reviews", movie_id)
    new_hash = compute_data_hash(reviews_data)

    try:
        old_hash = client.hget(key, "reviews_hash")
        if old_hash is None:
            return True  # Reviews mới, chưa từng cào
        return new_hash != old_hash
    except Exception as e:
        print(f"[Redis] Lỗi khi check reviews movie {movie_id}: {e}")
        return True


def save_reviews_state(source, movie_id, reviews_data, review_count=0, ttl_days=DEFAULT_TTL_DAYS):
    """
    Lưu trạng thái reviews vào Redis sau khi xử lý xong.
    Bao gồm: reviews_hash + thời gian cào cuối + số lượng reviews.
    TTL mặc định 30 ngày — hết hạn sẽ tự xóa → lần sau cào lại từ đầu.
    """
    client = get_redis_client()
    if client is None:
        return

    key = _get_key(source, "reviews", movie_id)
    state = {
        "reviews_hash": compute_data_hash(reviews_data),
        "last_scraped": datetime.now().isoformat(),
        "source": source,
        "movie_id": str(movie_id),
        "review_count": str(review_count)
    }

    try:
        client.hset(key, mapping=state)
        client.expire(key, ttl_days * 86400)
    except Exception as e:
        print(f"[Redis] Lỗi khi lưu state reviews movie {movie_id}: {e}")
