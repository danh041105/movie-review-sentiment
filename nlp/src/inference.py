"""
=============================================================================
INFERENCE - Chạy HÀNG NGÀY trong DAG Airflow
=============================================================================
Script này:
  1. Load model đã train sẵn từ MinIO (silver/models/sentiment_lr/)
  2. Đọc dữ liệu reviews mới của NGÀY HÔM NAY từ Silver
  3. Làm sạch text → predict sentiment bằng model
  4. Ghi kết quả enriched reviews vào silver/nlp/reviews_enriched/{date}/

Ưu điểm:
  - KHÔNG train lại model mỗi ngày → tiết kiệm tài nguyên
  - Chỉ xử lý dữ liệu mới → pipeline nhanh hơn rất nhiều
  - Model được quản lý tập trung tại 1 vị trí trên MinIO
=============================================================================
"""
import os
import sys
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from transformation.common.spark_utils import get_spark_session, write_data_to_minio, get_layer_path

# Hằng số tĩnh - không gọi os.getenv() hay datetime.now() ở module-level
BUCKET_NAME = "silver"
DATASET_PREFIX = "nlp/dataset"
MODEL_PATH = "s3a://silver/models/sentiment_lr/"
OUTPUT_PREFIX = "nlp/reviews_enriched"


def _clean_text(df):
    """
    Pipeline làm sạch văn bản review (giống hệt lúc train).
    QUAN TRỌNG: Phải giữ logic clean GIỐNG 100% với train_model.py
    để model predict chính xác.
    """
    clean_df = df.withColumn("clean_content", F.lower(F.col("content")))
    # Xóa thẻ HTML
    clean_df = clean_df.withColumn("clean_content", F.regexp_replace(F.col("clean_content"), "<.*?>", " "))
    # Chỉ giữ chữ cái, số, khoảng trắng
    clean_df = clean_df.withColumn("clean_content", F.regexp_replace(F.col("clean_content"), "[^a-z0-9\\s]", " "))
    # Xóa khoảng trắng thừa
    clean_df = clean_df.withColumn("clean_content", F.regexp_replace(F.col("clean_content"), "\\s+", " "))
    # Trim 2 đầu
    clean_df = clean_df.withColumn("clean_content", F.trim(F.col("clean_content")))
    # Loại dòng rỗng
    clean_df = clean_df.filter(F.length(F.col("clean_content")) > 2)
    return clean_df


def predict_daily_reviews(target_date=None):
    """
    Hàm chính chạy hàng ngày trong DAG Airflow.
    
    Flow:
      1. Load model đã train từ MinIO
      2. Đọc reviews của ngày target_date từ silver/nlp/dataset/{date}/
      3. Clean text → model.transform() → predict sentiment
      4. Ghi kết quả vào silver/nlp/reviews_enriched/{date}/
    
    Args:
        target_date: Ngày cần xử lý (YYYY-MM-DD). 
                     Mặc định = ngày hiện tại khi hàm CHẠY.
    """
    # Lấy target_date tại thời điểm CHẠY, không phải lúc Airflow parse file
    if target_date is None:
        target_date = datetime.now().strftime("%Y-%m-%d")

    spark = get_spark_session("SparkML_Inference")

    # ===== BƯỚC 1: Load model đã train sẵn =====
    print(f"[*] Đang load model từ: {MODEL_PATH}")
    try:
        model = PipelineModel.load(MODEL_PATH)
        print("[+] Load model thành công!")
    except Exception as e:
        print(f"[!] ❌ Không tìm thấy model tại {MODEL_PATH}")
        print(f"[!] Hãy chạy train_model.py trước để tạo model.")
        print(f"[!] Chi tiết lỗi: {e}")
        spark.stop()
        raise RuntimeError(f"Model không tồn tại tại {MODEL_PATH}. Chạy train_model.py trước!") from e

    # ===== BƯỚC 2: Đọc reviews mới của ngày hôm nay =====
    data_path = get_layer_path("s3a://", BUCKET_NAME, DATASET_PREFIX, target_date)
    print(f"[*] Đang đọc reviews ngày {target_date} từ: {data_path}")

    try:
        df = spark.read.parquet(data_path + "*.parquet")
    except Exception as e:
        print(f"[!] Không có dữ liệu reviews cho ngày {target_date}")
        print(f"[!] Chi tiết: {e}")
        spark.stop()
        return

    record_count = df.count()
    print(f"[*] Số reviews cần predict: {record_count}")

    if record_count == 0:
        print("[!] Không có reviews mới. Kết thúc.")
        spark.stop()
        return

    # ===== BƯỚC 3: Clean text + Gán label tạm (cho format đúng) =====
    df = df.withColumn("label", F.when(F.col("rating") >= 7, 1.0).otherwise(0.0))
    clean_df = _clean_text(df)
    print(f"[*] Sau khi làm sạch: {clean_df.count()} reviews")

    # ===== BƯỚC 4: Predict sentiment =====
    print("[*] Đang chạy AI predict sentiment...")
    predictions = model.transform(clean_df)

    # Chọn các cột cần thiết
    final_df = predictions.select(
        "ingestion_id",
        "review_id",
        "tmdb_id",
        "imdb_id",
        "author",
        "content",
        "rating",
        "source_system",
        "created_at",
        F.col("prediction").alias("predicted_sentiment"),
        F.array_max(vector_to_array(F.col("probability"))).alias("sentiment_confidence")
    )

    final_count = final_df.count()
    print(f"[*] Đã predict xong {final_count} reviews")

    # ===== BƯỚC 5: Ghi kết quả =====
    write_data_to_minio(final_df, BUCKET_NAME, OUTPUT_PREFIX, target_date)
    print(f"[+] ✅ Hoàn tất inference ngày {target_date}: {final_count} reviews")

    spark.stop()


if __name__ == "__main__":
    # Cho phép truyền ngày từ command line
    if len(sys.argv) > 1:
        predict_daily_reviews(sys.argv[1])
    else:
        predict_daily_reviews()
