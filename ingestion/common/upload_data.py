import os
import json
import uuid
import hashlib
from minio import Minio
import io
from datetime import datetime, timedelta
from dotenv import load_dotenv
from ingestion.common.schema import Schema
load_dotenv()

# Thiết lập kết nối đến MinIO
client = Minio(
    endpoint= os.getenv("MINIO_ENDPOINT"),
    access_key=os.getenv("MINIO_ROOT_USER"),
    secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
    secure=False
    )

#Tạo bucket
BUCKET_NAME = "bronze"
if not client.bucket_exists(BUCKET_NAME):
    client.make_bucket(bucket_name=BUCKET_NAME)

def upload_to_minio(raw_data, source, entity, methods, http_status, search_params):
    if not raw_data:
        print("Không có dữ liệu để xử lý")
        return
    now = datetime.now()
    partition = f"{source}/{entity}/{now.year}/{now.month:02d}/{now.day:02d}"
    if entity == "movies":
        file_list = [raw_data] 
        file_naming_convention = lambda i: f"raw_movies.jsonl"
    elif entity == "reviews":
        movie_id = search_params.get("movie_id")
        chunk_size = 1000
        file_list = [raw_data[i:i + chunk_size] for i in range(0, len(raw_data), chunk_size)]   
        if movie_id:
            partition += f"/{movie_id}"
        timestamp = now.strftime('%H%M%S')
        file_naming_convention = lambda i: f"raw_reviews_part_{i+1}.jsonl"
    
    for index, data_chunk in enumerate(file_list):
        object_key = f"{partition}/{file_naming_convention(index)}"
        jsonl_content = ""
        for item in data_chunk:
            record = Schema(
                raw_data = item,
                source = source,
                entity = entity,
                methods = methods,
                http_status = http_status,
                search_params = search_params
            ).build_schema()
            jsonl_content += json.dumps(record) + "\n"

        data_payload = jsonl_content.encode('utf-8')
        data_stream = io.BytesIO(data_payload)
        data_size = len(data_payload)
        try:
            client.put_object(
                bucket_name=BUCKET_NAME,
                object_name=object_key,
                data= data_stream,
                length=data_size,
                content_type="application/x-jsonlines"
            )
            print(f"Đã đẩy dữ liệu thành công lên: {BUCKET_NAME}/{object_key}")
        except Exception as e:
            print(f"Lỗi upload MinIO: {e}")
