import os
import json
import uuid
import hashlib
from minio import Minio
import io
from datetime import datetime, timedelta
from dotenv import load_dotenv
from schema import Schema
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
    partition = f"{source}/{entity}/year={now.year}/month={now.month:02d}/day={now.day:02d}"
    file_name = f"trending_{now.strftime('%H%M%S')}.jsonl"
    object_key = f"{partition}/{file_name}"

    jsonl_content = ""
    for item in raw_data:
        record = Schema(
            raw_data = item,
            source = source,
            entity = entity,
            methods = methods,
            http_status = http_status,
            search_params = search_params
        ).build_schema()
        jsonl_content += json.dumps(record) + "\n"

    data_stream = io.BytesIO(jsonl_content.encode('utf-8'))
    data_size = len(jsonl_content.encode('utf-8'))

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
