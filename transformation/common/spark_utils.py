import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv
from datetime import datetime
load_dotenv()

def get_spark_session(app_name='MovieTransformation'):
    spark = SparkSession.builder \
        .appName(app_name) \
        .master(os.getenv("SPARK_URL")) \
        .config('spark.hadoop.fs.s3a.endpoint', os.getenv("MINIO_ENDPOINT")) \
        .config('spark.hadoop.fs.s3a.access.key', os.getenv('MINIO_ROOT_USER')) \
        .config('spark.hadoop.fs.s3a.secret.key', os.getenv('MINIO_ROOT_PASSWORD')) \
        .config('spark.hadoop.fs.s3a.path.style.access', 'true') \
        .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')
    return spark

def read_daily_data_from_minio(spark, bucket_name, base_prefix, target_date=None):
    """
    Spark đọc trực tiếp toàn bộ file JSONL của một ngày cụ thể từ MinIO.
    Args:
        spark: SparkSession đã được cấu hình s3a.
        bucket_name (str): Tên bucket (VD: 'bronze').
        base_prefix (str): Đường dẫn gốc (VD: 'tmdb/movies').
        target_date (str, optional): Ngày cần lấy (YYYY-MM-DD). Mặc định là hôm nay.
    Returns:
        DataFrame: Dữ liệu thô đã nạp vào RAM. Trả về None nếu thư mục không tồn tại/trống.
    """
    if target_date is None:
        target_date = datetime.now().strftime("%Y-%m-%d")
    s3a_path = f"s3a://{bucket_name}/{base_prefix}/{target_date}/*.jsonl"
    print(f"[*] Spark đang quét và nạp dữ liệu từ: {s3a_path}")
    try:
        df = spark.read.json(s3a_path)
        print(f"[*] Thành công! Đã nạp {df.count()} dòng dữ liệu.")
        return df
    except Exception as e:
        print(f"[!] Không tìm thấy dữ liệu hoặc có lỗi xảy ra tại {s3a_path}")
        print(f"Chi tiết lỗi: {e}")
        return None