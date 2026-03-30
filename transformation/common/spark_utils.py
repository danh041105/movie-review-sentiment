import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv
from datetime import datetime
load_dotenv()
from minio import Minio
os.environ["HADOOP_HOME"] = "D:/hadoop"
os.environ["PATH"] = os.environ["PATH"] + ";D:/hadoop/bin"

def get_spark_session(app_name='MovieTransformation'):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.4.1,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT")) \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def read_daily_data_from_minio(spark, bucket_name, base_prefix, target_date=None):
    if target_date is None:
        target_date = datetime.now().strftime("%Y-%m-%d")
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
    date_path = target_date.replace("-", "/")
    
    if "reviews" in base_prefix:
        s3a_path = f"s3a://{bucket_name}/{base_prefix}/{date_path}/*/*.jsonl"
    else: 
        s3a_path = f"s3a://{bucket_name}/{base_prefix}/{date_path}/*.jsonl"
    print(f"[*] Spark đang quét và nạp dữ liệu từ: {s3a_path}")
    try:
        df = spark.read.json(s3a_path)
        print(f"[*] Thành công! Đã nạp {df.count()} dòng dữ liệu.")
        return df
    except Exception as e:
        print(f"[!] Không tìm thấy dữ liệu hoặc có lỗi xảy ra tại {s3a_path}")
        print(f"Chi tiết lỗi: {e}")
        return None
    
def ensure_bucket_exists(bucket_name):
    client = Minio(
        os.getenv("MINIO_ENDPOINT"),
        access_key=os.getenv("MINIO_ROOT_USER"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
        secure=False
    )
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"[*] Đã tạo bucket: {bucket_name}")

def get_layer_path(source, bucket_name, base_prefix, target_date=None):
    if target_date is None:
        target_date = datetime.now().strftime("%Y-%m-%d")
    ensure_bucket_exists(bucket_name)
    date_path = target_date.replace("-", "/")
    output_path = f"{source}{bucket_name}/{base_prefix}/{date_path}/"
    return output_path

def write_data_to_minio(df, bucket_name, base_prefix, target_date=None, mode="overwrite", file_format="parquet"):
    output_path = get_layer_path("s3a://", bucket_name, base_prefix, target_date)
    print(f"[*] Đang ghi dữ liệu ({df.count()} dòng) xuống: {output_path}")
    total_rows = df.count()
    if total_rows < 50000:
        n_parts = 1  # Dữ liệu nhỏ thì gom về 1 file
    elif total_rows < 500000:
        n_parts = 4  # Dữ liệu vừa
    else:
        n_parts = 10 # Dữ liệu lớn hơn
        
    print(f"[*] Tự động điều chỉnh: {total_rows} dòng -> {n_parts} partitions")    
    print(f"[*] Đang ghi dữ liệu ({df.count()} dòng) xuống: {output_path}")
    try:
        df.coalesce(n_parts).write.mode(mode).format(file_format).save(output_path)
        print("[*] Ghi dữ liệu thành công!")
    except Exception as e:
        print(f"[!] Lỗi khi ghi dữ liệu: {e}")