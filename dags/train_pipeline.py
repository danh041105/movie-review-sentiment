"""
=============================================================================
TRAIN MODEL DAG - Chạy ĐỊNH KỲ 2 lần/tháng (ngày 1 và ngày 15)
=============================================================================
DAG này:
  1. Train lại model Logistic Regression trên TOÀN BỘ dữ liệu tích lũy
  2. Đánh giá model (ROC-AUC, F1, Precision, Recall, Confusion Matrix)
  3. Lưu PipelineModel vào MinIO: silver/models/sentiment_lr/

Tách riêng khỏi pipeline chính (daily) vì:
  - Training tốn tài nguyên hơn inference rất nhiều
  - Model không cần train lại mỗi ngày
  - Cho phép trigger thủ công khi cần retrain gấp

Lưu ý: DAG daily pipeline (ml_inference) phụ thuộc vào model đã được
train và lưu sẵn tại s3a://silver/models/sentiment_lr/
Nếu model chưa tồn tại, inference sẽ báo lỗi.
=============================================================================
"""
import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from common import telegram_notifier

AIRFLOW_HOME = Path(__file__).resolve().parent.parent
if str(AIRFLOW_HOME) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_HOME))

from nlp.src.train_model import train_and_save_model

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=3),  
    "depends_on_past": False,
    "on_failure_callback": telegram_notifier.task_fail_alert
}

with DAG(
    dag_id="train_sentiment_model",
    default_args=default_args,
    description="Train lại model Logistic Regression cho Sentiment Analysis (2 lần/tháng)",
    # Cron: chạy lúc 2h sáng ngày 1 và ngày 15 hàng tháng
    schedule_interval="0 2 1,15 * *",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["ml", "training", "sentiment", "spark"],
    dagrun_timeout=timedelta(hours=4),
    on_success_callback=telegram_notifier.dag_success_alert
) as dag:

    start = EmptyOperator(task_id="start_training")

    train_model_task = PythonOperator(
        task_id="train_sentiment_model",
        python_callable=train_and_save_model,
        retries=2,
        retry_delay=timedelta(minutes=10),       # Chờ lâu hơn vì Spark cần giải phóng tài nguyên
        execution_timeout=timedelta(hours=3),    # Training trên toàn bộ dataset có thể lâu
    )

    end = EmptyOperator(task_id="training_completed")

    start >> train_model_task >> end
