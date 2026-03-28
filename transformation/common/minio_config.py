from minio import Minio
from datetime import datetime
from pyspark.sql import SparkSession
import os
import sys

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

def get_minio_client():
    client = Minio(
        MINIO_ENDPOINT,
        access_key = MINIO_ACCESS_KEY,
        secret_key = MINIO_SECRET_KEY,
        secure = False # Đặt False vì đang chạy local, không sử dụng HTTPS(phù hợp với local)
    )
    return client

def get_daily_data(client, bucket_name, base_prefix, target_date=None):
    """
    Lấy danh sách đường dẫn các file .jsonl trên MinIO theo một ngày cụ thể.
    Args:
        client (Minio): Đối tượng MinIO client đã kết nối.
        bucket_name (str): Tên bucket (VD: 'bronze').
        base_prefix (str): Đường dẫn thư mục gốc (VD: 'tmdb/movies').
        target_date (str, optional): Ngày cần lấy dữ liệu định dạng YYYY-MM-DD. 
                                     Mặc định sẽ tự động lấy ngày hôm nay.
    Returns:
        list: Danh sách các đường dẫn object (object_name) thỏa mãn điều kiện.
    """
    if target_date is None:
        target_date = datetime.now().strftime("%Y-%m-%d")
    # Xây dựng prefix tìm kiếm dựa trên ngày
    # Giả định dữ liệu lưu theo cấu trúc: base_prefix/YYYY-MM-DD/
    # VD: tmdb/movies/2026-03-26/
    search_prefix = f"{base_prefix}/{target_date}"  
    print(f"[*] Đang quét bucket '{bucket_name}' với đường dẫn: {search_prefix}")
    client = get_minio_client()
    objects = client.list_objects(bucket_name, prefix=search_prefix, recursive=True)

    file_paths = [obj.object_name for obj in objects if obj.object_name.endswith(".jsonl")]
    return file_paths
