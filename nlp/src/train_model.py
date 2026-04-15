"""
=============================================================================
TRAIN MODEL - Chạy ĐỊNH KỲ (1 lần/tháng hoặc khi cần retrain)
=============================================================================
Script này:
  1. Đọc TOÀN BỘ dữ liệu NLP dataset tích lũy từ Silver (tất cả các ngày)
  2. Train Logistic Regression trên toàn bộ dataset
  3. Đánh giá AUC trên tập test
  4. Lưu PipelineModel vào MinIO: silver/models/sentiment_lr/
  
Cách chạy:
  - Thủ công: python train_model.py
  - Hoặc trigger DAG riêng (ví dụ: @monthly)
=============================================================================
"""
import os
import sys
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer, IDF
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml import Pipeline

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from transformation.common.spark_utils import get_spark_session, get_layer_path

# Hằng số tĩnh - không gọi os.getenv() hay datetime.now() ở module-level
BUCKET_NAME = "silver"
DATASET_PREFIX = "nlp/dataset"
MODEL_PATH = "s3a://silver/models/sentiment_lr/"


def _clean_text(df):
    """
    Pipeline làm sạch văn bản review trước khi đưa vào model.
    Bao gồm: lowercase, xóa HTML, xóa ký tự đặc biệt, trim.
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


def train_and_save_model():
    """
    Hàm chính: Train model trên TOÀN BỘ dữ liệu tích lũy và lưu vào MinIO.
    """
    spark = get_spark_session("SparkML_TrainModel")

    # ===== BƯỚC 1: Đọc TOÀN BỘ dataset (tất cả các ngày) =====
    all_data_path = f"s3a://{BUCKET_NAME}/{DATASET_PREFIX}/*/*/*/*.parquet"
    print(f"[*] Đang nạp TOÀN BỘ dữ liệu training từ: {all_data_path}")

    try:
        df = spark.read.parquet(all_data_path)
    except Exception as e:
        print(f"[!] Lỗi khi đọc dữ liệu: {e}")
        spark.stop()
        return

    # ===== BƯỚC 2: Gán label + Làm sạch =====
    df = df.withColumn("label", F.when(F.col("rating") >= 7, 1.0).otherwise(0.0))
    clean_df = _clean_text(df)

    # Chỉ lấy những dòng có rating (có label rõ ràng) để train
    labeled_df = clean_df.filter(F.col("rating").isNotNull())

    # Dedup theo review_id để tránh data trùng lặp giữa các ngày
    labeled_df = labeled_df.dropDuplicates(["review_id"])

    # Cache để tránh đọc lại từ S3 nhiều lần
    labeled_df.cache()

    # ===== BƯỚC 3: Chia Train/Test =====
    train, test = labeled_df.randomSplit([0.8, 0.2], seed=42)
    print("[*] Đã chia Train/Test (80/20)")

    # ===== BƯỚC 4: Xây dựng Pipeline ML =====
    print("[*] Đang xây dựng ML Pipeline...")
    tokenizer = RegexTokenizer(inputCol="clean_content", outputCol="words", pattern="\\W")
    remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
    cv = CountVectorizer(inputCol="filtered_words", outputCol="raw_features", vocabSize=10000)
    idf = IDF(inputCol="raw_features", outputCol="features")
    lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=20, regParam=0.05)

    pipeline = Pipeline(stages=[tokenizer, remover, cv, idf, lr])

    print("[*] Đang huấn luyện model (có thể mất vài phút với dữ liệu lớn)...")
    model = pipeline.fit(train)
    print("[+] Huấn luyện hoàn tất!")

    # ===== BƯỚC 5: Đánh giá =====
    test_predictions = model.transform(test)
    evaluator = BinaryClassificationEvaluator(
        rawPredictionCol="rawPrediction", labelCol="label", metricName="areaUnderROC"
    )
    auc = evaluator.evaluate(test_predictions)
    print(f"[*] CHỈ SỐ ROC-AUC: {auc:.4f}")
    if auc > 0.8:
        print("   -> Mô hình đạt chất lượng TỐT, sẵn sàng deploy!")
    else:
        print("   -> Cảnh báo: AUC thấp, cần kiểm tra lại dữ liệu hoặc tham số.")

    # ===== BƯỚC 6: Lưu Model vào MinIO =====
    print(f"[*] Đang lưu model vào: {MODEL_PATH}")
    model.write().overwrite().save(MODEL_PATH)
    print(f"[+] ✅ Model đã được lưu thành công tại: {MODEL_PATH}")
    print(f"[+] Thời gian train: {datetime.now().isoformat()}")
    print(f"[+] AUC = {auc:.4f}")

    # Giải phóng cache
    labeled_df.unpersist()
    spark.stop()


if __name__ == "__main__":
    train_and_save_model()
