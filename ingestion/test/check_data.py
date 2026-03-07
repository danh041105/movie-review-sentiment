from minio import Minio
import os
import json

client = Minio(
    endpoint= os.getenv("MINIO_ENDPOINT"),
    access_key=os.getenv("MINIO_ROOT_USER"),
    secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
    secure=False
    )

bucket_name = "bronze"
object_name = "trending_211822.jsonl"

response = client.get_object(bucket_name, object_name)

for line in response:
    data = json.loads(line.decode("utf-8"))
    print(data)