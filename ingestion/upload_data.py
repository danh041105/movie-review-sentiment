import os
import json
import uuid
import hashlib
from minio import Minio
import io
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

# Thiết lập kết nối đến MinIO
client = Minio(
    endpoint= os.getenv("MINIO_ENDPOINT"),
    access_key=os.getenv("MINIO_ROOT_USER"),
    secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
    secure=False
    )
# print("Total buckets:", len(list(client.list_buckets())))

#Tạo bucket
BUCKET_NAME = "bronze"
if not client.bucket_exists(BUCKET_NAME):
    client.make_bucket(bucket_name=BUCKET_NAME)

def upload_to_minio(movies, source, entity, http_status, search_params):
    if not movies:
        print("Không có dữ liệu để xử lý")
        return
    
    today = datetime.now()
    partition = f"{source}/{entity}/year={today.year}/month={today.month:02d}/day={today.day:02d}"
    file_name = f"trending_{today.strftime('%H%M%S')}.jsonl"
    object_key = f"{partition}/{file_name}"

    jsonl_content = ""
    for movie in movies:
        schema = {
            "metadata": {
                "ingestion_id": str(uuid.uuid4()),
                "source_system": source,
                "entity": entity,
                "extraction_method": "API Calling",
                "ingestion_timestamp": today.isoformat(),
                "http_status": http_status,
                "search_parameters": search_params,
                "raw_hash": hashlib.sha256(json.dumps(movie, sort_keys=True).encode()).hexdigest()
            },
            "raw_payload": movie
        }
        jsonl_content += json.dumps(schema) + "\n"

    data_stream = io.BytesIO(jsonl_content.encode('utf-8'))

    try:
        client.put_object(
            bucket_name=BUCKET_NAME,
            object_name=object_key,
            data= data_stream,
            length=len(jsonl_content.encode('utf-8')),
            content_type="application/x-jsonlines"
        )
        print(f"Đã đẩy dữ liệu thành công lên: {BUCKET_NAME}/{object_key}")
    except Exception as e:
        print(f"Lỗi upload MinIO: {e}")
