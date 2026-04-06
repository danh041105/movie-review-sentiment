import os
import sys
import re
from pyspark.sql import functions as F
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer, IDF
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml import Pipeline
from datetime import datetime
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from transformation.common.spark_utils import get_spark_session, write_data_to_minio, get_layer_path

endpoint_url = os.getenv("ENDPOINT_URL")
key = os.getenv("MINIO_ROOT_USER")
secret = os.getenv("MINIO_ROOT_PASSWORD")
bucket_name = "silver"
base_prefix = "nlp/dataset"
target_date = datetime.now().strftime("%Y-%m-%d")

def load_data_for_nlp(s3a_path):
    spark = get_spark_session("SparkML_Pipeline")
    print(f"[*] Đang kết nối tới MinIO tại {endpoint_url}...")
    print(f"[*] Nạp dữ liệu từ: {s3a_path}")
    try:
        df = spark.read.parquet(s3a_path)
        return df
    except Exception as e:
        print(f"[!] Lỗi khi nạp dữ liệu trực tiếp: {e}")
        return None
    
def get_clean_df():
    s3_path = get_layer_path("s3a://", bucket_name, base_prefix, target_date) 
    print(s3_path)
    df = load_data_for_nlp(s3_path + "*.parquet")
    df = df.withColumn("label", F.when(F.col("rating") >= 7, 1.0).otherwise(0.0))
    clean_df = df.withColumn("clean_content", F.lower(F.col("content")))
# 2. Xóa thẻ HTML (VD: <br />, <i>)
    clean_df = clean_df.withColumn("clean_content", F.regexp_replace(F.col("clean_content"), "<.*?>", " "))
    # 3. Xóa các ký tự đặc biệt, chỉ giữ lại chữ cái a-z, số 0-9 và khoảng trắng
    clean_df = clean_df.withColumn("clean_content", F.regexp_replace(F.col("clean_content"), "[^a-z0-9\\s]", " "))
    # 4. Xóa khoảng trắng thừa (nhiều dấu cách liền nhau chuyển thành 1 dấu cách)
    clean_df = clean_df.withColumn("clean_content", F.regexp_replace(F.col("clean_content"), "\\s+", " "))
    # 5. Xóa khoảng trắng ở 2 đầu
    clean_df = clean_df.withColumn("clean_content", F.trim(F.col("clean_content")))
    # 6. Loại bỏ những dòng rỗng (sau khi clean bị mất hết chữ)
    clean_df = clean_df.filter(F.length(F.col("clean_content")) > 2)
    print(f"[*] Số lượng bản ghi giữ lại sau khi làm sạch: {clean_df.count()}")
    return clean_df

def train_sentiment_model():
    clean_df = get_clean_df()
    labeled_df = clean_df.filter(F.col("rating").isNotNull())
    train, test = labeled_df.randomSplit([0.8, 0.2], seed=42)
    print(f"[*] Số mẫu Huấn luyện (Train): {train.count()}")
    print(f"[*] Số mẫu Kiểm thử (Test): {test.count()}")
    print("BẮT ĐẦU XÂY DỰNG PIPELINE VÀ HUẤN LUYỆN...")
# 1. Các bước NLP biến đổi chữ thành số
    tokenizer = RegexTokenizer(inputCol="clean_content", outputCol="words", pattern="\\W")
    remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
    cv = CountVectorizer(inputCol="filtered_words", outputCol="raw_features", vocabSize=10000)
    idf = IDF(inputCol="raw_features", outputCol="features")

    # 2. Thuật toán Hồi quy Logistic
    lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=20, regParam=0.05)
    # 3. Gom thành 1 dây chuyền (Pipeline)
    pipeline = Pipeline(stages=[tokenizer, remover, cv, idf, lr])
    print("Đang huấn luyện mô hình (mất khoảng vài chục giây)...")
    model = pipeline.fit(train)
    print("Huấn luyện hoàn tất!")
    print("BẮT ĐẦU ĐÁNH GIÁ MÔ HÌNH...")
# Dự đoán trên tập Test
    test_predictions = model.transform(test)

    # Chấm điểm
    evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction", labelCol="label", metricName="areaUnderROC")
    auc = evaluator.evaluate(test_predictions)
    print(f"CHỈ SỐ ROC-AUC ĐẠT ĐƯỢC: {auc:.4f}")
    if auc > 0.8:
        print("   -> Đánh giá: Mô hình học rất tốt, sẵn sàng sử dụng!")
    else:
        print("   -> Đánh giá: Cần kiểm tra lại dữ liệu hoặc điều chỉnh tham số.")
    print("BẮT ĐẦU DÙNG AI CHẤM ĐIỂM CHO TOÀN BỘ DỮ LIỆU...")
    # all_predictions được tạo ra bằng cách ném TOÀN BỘ clean_df vào model đã train
    # Lưu ý: clean_df bao gồm cả 2275 dòng bị NULL rating
    all_predictions = model.transform(clean_df)

    # Kiểm tra thử kết quả dự đoán của 2275 dòng NULL đó
    # print("Kết quả AI dự đoán cho những dòng vốn bị NULL rating:")
    # all_predictions.filter(F.col("rating").isNull()) \
    #                .select("content", "prediction", "probability") \
    #                .show(5, truncate=False)
    # Bước cuối: Chọn các cột cần thiết để lưu trữ
    from pyspark.ml.functions import vector_to_array
    F.col("probability")
    final_df = all_predictions.select(
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
    output_path = "nlp/reviews_enriched"
    write_data_to_minio(final_df, bucket_name, output_path, target_date)

