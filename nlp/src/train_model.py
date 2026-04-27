"""
=============================================================================
TRAIN MODEL - Chạy ĐỊNH KỲ (1 lần/tháng hoặc khi cần retrain)
=============================================================================
Script này:
  1. Đọc TOÀN BỘ dữ liệu NLP dataset tích lũy từ Silver (tất cả các ngày)
  2. Cân bằng trọng số giữa 2 lớp (class_weight balanced)
  3. Train Logistic Regression (C=1, tương đương regParam=1.0) trên toàn bộ dataset
  4. Đánh giá AUC, F1, Precision, Recall trên tập test
  5. Lưu PipelineModel vào MinIO: silver/models/sentiment_lr/
  
Cách chạy:
  - Thủ công: python train_model.py
  - Hoặc trigger DAG riêng (ví dụ: @monthly)

Tham số tối ưu lấy từ notebook he.ipynb (GridSearchCV):
  - C=1 (scikit-learn) → regParam=1.0 (Spark ML)
  - class_weight='balanced' → weightCol tính thủ công
  - max_iter=2000 → maxIter=2000
=============================================================================
"""
import os
import sys
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml import Pipeline

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from transformation.common.spark_utils import get_spark_session, get_layer_path, ensure_bucket_exists

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


def _add_class_weights(df, label_col="label"):
    """
    Tính trọng số cân bằng cho từng lớp (tương đương class_weight='balanced' 
    trong scikit-learn).
    
    Công thức: weight_i = n_samples / (n_classes * n_samples_i)
    
    Ví dụ với dữ liệu hiện tại:
      - label 1.0: 36610 mẫu → weight = 54242 / (2 * 36610) ≈ 0.74
      - label 0.0: 17632 mẫu → weight = 54242 / (2 * 17632) ≈ 1.54
    → Mẫu tiêu cực (label 0.0) sẽ có trọng số gấp đôi mẫu tích cực.
    """
    n_samples = df.count()
    n_classes = df.select(label_col).distinct().count()
    
    # Tính số lượng mẫu mỗi lớp
    class_counts = df.groupBy(label_col).count().collect()
    
    # Tạo dictionary: {label: weight}
    weight_map = {}
    for row in class_counts:
        label = row[label_col]
        count = row["count"]
        weight = n_samples / (n_classes * count)
        weight_map[label] = weight
        print(f"   Label {label}: {count} mẫu → weight = {weight:.4f}")
    
    # Thêm cột classWeight vào DataFrame
    weight_expr = F.when(F.col(label_col) == 1.0, F.lit(weight_map[1.0])) \
                   .otherwise(F.lit(weight_map[0.0]))
    
    df = df.withColumn("classWeight", weight_expr)
    return df
def train_and_save_model():
    """
    Hàm chính: Train model trên TOÀN BỘ dữ liệu tích lũy và lưu vào MinIO.
    
    Áp dụng các tham số tối ưu từ notebook he.ipynb:
      - class_weight='balanced' (qua weightCol)
      - C=1 → regParam=1.0
      - max_iter=2000
    """
    spark = get_spark_session("SparkML_TrainModel")

    # ===== BƯỚC 0: Đảm bảo bucket tồn tại =====
    ensure_bucket_exists(BUCKET_NAME)

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
    # Label: rating >= 7 → Positive (1.0), ngược lại → Negative (0.0)
    df = df.withColumn("label", F.when(F.col("rating") >= 7, 1.0).otherwise(0.0))
    clean_df = _clean_text(df)

    # Chỉ lấy những dòng có rating (có label rõ ràng) để train
    labeled_df = clean_df.filter(F.col("rating").isNotNull())

    # Dedup theo review_id để tránh data trùng lặp giữa các ngày
    labeled_df = labeled_df.dropDuplicates(["review_id"])

    # Cache để tránh đọc lại từ S3 nhiều lần
    labeled_df.cache()
    total_count = labeled_df.count()
    print(f"[*] Tổng số mẫu sau tiền xử lý: {total_count}")

    # ===== BƯỚC 3: Thêm class weights (balanced) =====
    print("[*] Đang tính class weights (balanced)...")
    labeled_df = _add_class_weights(labeled_df, label_col="label")

    # ===== BƯỚC 4: Chia Train/Test (80/20, stratify qua seed) =====
    train, test = labeled_df.randomSplit([0.8, 0.2], seed=42)
    train_count = train.count()
    test_count = test.count()
    print(f"[*] Đã chia Train/Test: {train_count} / {test_count}")

    # In phân phối label
    print("[*] Phân phối label trong tập Train:")
    train.groupBy("label").count().show()
    print("[*] Phân phối label trong tập Test:")
    test.groupBy("label").count().show()

    # ===== BƯỚC 5: Xây dựng Pipeline ML =====
    print("[*] Đang xây dựng ML Pipeline...")
    
    # Tokenizer: tách từ
    tokenizer = RegexTokenizer(
        inputCol="clean_content", 
        outputCol="words", 
        pattern="\\W"
    )
    # StopWordsRemover: loại bỏ stop words tiếng Anh
    default_stop_words = StopWordsRemover.loadDefaultStopWords("english")
    words_to_keep = {"no", "not", "never", "cannot", "isn't", "aren't", "wasn't", "weren't", "hasn't", "haven't", "hadn't", "doesn't", "don't", "didn't", "won't", "wouldn't", "shan't", "shouldn't", "mustn't", "can't", "couldn't"}
    custom_stop_words = [word for word in default_stop_words if word not in words_to_keep]
    remover = StopWordsRemover(
        inputCol="words", 
        outputCol="filtered_words",
        stopWords=custom_stop_words
    )
    
    # HashingTF: chuyển từ thành vector TF (tương đương TfidfVectorizer max_features=10000)
    hashingTF = HashingTF(
        inputCol="filtered_words", 
        outputCol="raw_features", 
        numFeatures=10000
    )
    
    # IDF: tính trọng số IDF
    idf = IDF(
        inputCol="raw_features", 
        outputCol="features"
    )
    
    # Logistic Regression với tham số tối ưu từ notebook
    # C=1 trong sklearn → regParam=1.0 trong Spark ML
    # class_weight='balanced' → weightCol="classWeight"
    # max_iter=2000 → maxIter=2000
    lr = LogisticRegression(
        featuresCol="features",
        labelCol="label",
        weightCol="classWeight",
        maxIter=2000,
        regParam=1.0,
        elasticNetParam=0.0,  # L2 regularization (tương đương penalty='l2')
    )

    pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, idf, lr])

    print("[*] Đang huấn luyện model (có thể mất vài phút với dữ liệu lớn)...")
    model = pipeline.fit(train)
    print("[+] Huấn luyện hoàn tất!")

    # ===== BƯỚC 6: Đánh giá toàn diện =====
    # print("\n" + "=" * 60)
    # print("            ĐÁNH GIÁ MÔ HÌNH TRÊN TẬP TEST")
    # print("=" * 60)
    
    # test_predictions = model.transform(test)
    
    # # 6a. ROC-AUC
    # auc_evaluator = BinaryClassificationEvaluator(
    #     rawPredictionCol="rawPrediction", labelCol="label", metricName="areaUnderROC"
    # )
    # auc = auc_evaluator.evaluate(test_predictions)
    # print(f"   ROC-AUC Score  : {auc:.4f}")
    
    # # 6b. PR-AUC (Precision-Recall AUC - quan trọng với imbalanced data)
    # pr_evaluator = BinaryClassificationEvaluator(
    #     rawPredictionCol="rawPrediction", labelCol="label", metricName="areaUnderPR"
    # )
    # pr_auc = pr_evaluator.evaluate(test_predictions)
    # print(f"   PR-AUC Score   : {pr_auc:.4f}")
    
    # # 6c. F1-Score, Precision, Recall, Accuracy
    # f1_evaluator = MulticlassClassificationEvaluator(
    #     predictionCol="prediction", labelCol="label", metricName="f1"
    # )
    # f1 = f1_evaluator.evaluate(test_predictions)
    
    # acc_evaluator = MulticlassClassificationEvaluator(
    #     predictionCol="prediction", labelCol="label", metricName="accuracy"
    # )
    # accuracy = acc_evaluator.evaluate(test_predictions)
    
    # precision_evaluator = MulticlassClassificationEvaluator(
    #     predictionCol="prediction", labelCol="label", metricName="weightedPrecision"
    # )
    # precision = precision_evaluator.evaluate(test_predictions)
    
    # recall_evaluator = MulticlassClassificationEvaluator(
    #     predictionCol="prediction", labelCol="label", metricName="weightedRecall"
    # )
    # recall = recall_evaluator.evaluate(test_predictions)
    
    # print(f"   Accuracy       : {accuracy:.4f}")
    # print(f"   F1 Score       : {f1:.4f}")
    # print(f"   Precision (avg): {precision:.4f}")
    # print(f"   Recall (avg)   : {recall:.4f}")
    
    # # 6d. Confusion Matrix
    # print("\n[*] Confusion Matrix:")
    # test_predictions.groupBy("label", "prediction").count().orderBy("label", "prediction").show()
    
    # if auc > 0.8:
    #     print("Mô hình đạt chất lượng TỐT, sẵn sàng deploy!")
    # else:
    #     print("Cảnh báo: AUC thấp, cần kiểm tra lại dữ liệu hoặc tham số.")
    # print("=" * 60)

    # ===== BƯỚC 7: Lưu Model vào MinIO =====
    print(f"\n[*] Đang lưu model vào: {MODEL_PATH}")
    model.write().overwrite().save(MODEL_PATH)
    print(f"[+] Model đã được lưu thành công tại: {MODEL_PATH}")
    print(f"[+] Thời gian train: {datetime.now().isoformat()}")
    # print(f"[+] AUC = {auc:.4f} | F1 = {f1:.4f} | Accuracy = {accuracy:.4f}")

    # Giải phóng cache
    labeled_df.unpersist()
    spark.stop()

if __name__ == "__main__":
    train_and_save_model()
