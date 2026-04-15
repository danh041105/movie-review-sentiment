import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


AIRFLOW_HOME = Path(__file__).resolve().parent.parent
if str(AIRFLOW_HOME) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_HOME))

from ingestion import imdb_run, tmdb_run
from transformation import merge_reviews_silver, movies_run, reviews_run
from nlp.src import inference
from gold.gold_run import run_gold_layer

# ==============================================================================
# CẤU HÌNH RETRY CHUNG (áp dụng cho tất cả tasks nếu không override)
# ==============================================================================
default_args = {
    "owner": "airflow",
    "retries": 2,                               # Mặc định retry 2 lần
    "retry_delay": timedelta(minutes=3),         # Chờ 3 phút giữa các lần retry
    "retry_exponential_backoff": True,           # Tăng thời gian chờ theo cấp số nhân (3p → 6p → 12p)
    "max_retry_delay": timedelta(minutes=30),    # Tối đa chờ 30 phút giữa các lần retry
    "execution_timeout": timedelta(hours=2),     # Mỗi task tối đa chạy 2 tiếng
    "depends_on_past": False,                    # Không phụ thuộc vào lần chạy trước
}

with DAG(
    dag_id = "movie_sentiment_full_pipeline",
    default_args = default_args,
    schedule_interval='@daily',
    start_date=datetime(2026, 4, 3),
    catchup=False,
    tags=['movie', 'sentiment', 'spark', 'nlp'],
    dagrun_timeout=timedelta(hours=6),           # Toàn bộ DAG run tối đa 6 tiếng
) as dag:
    
    # ==================================================================
    # STAGE 1: INGESTION (retry 3 lần — API bên ngoài hay bị timeout)
    # ==================================================================
    start_pipeline = EmptyOperator(task_id='start_pipeline')

    ingest_imdb_task = PythonOperator(
        task_id = "ingest_imdb_data",
        python_callable = imdb_run.main,
        op_kwargs={
            "movie_limit": 100,
            "review_per_movie": 2000,
            "max_worker": 15
        },
        retries=3,                                  # Override: API IMDB hay lỗi → retry nhiều hơn
        retry_delay=timedelta(minutes=2),            # Chờ ít hơn vì lỗi mạng thường nhanh hồi phục
        execution_timeout=timedelta(hours=3),        # Cào dữ liệu có thể lâu
    )

    ingest_tmdb_task = PythonOperator(
        task_id = "ingest_tmdb_data",
        python_callable = tmdb_run.main,
        op_kwargs={
            "movie_limit": 100,
            "review_per_movie": 2000,
            "max_worker": 5
        },
        retries=3,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(hours=3),
    )

    end_of_ingestion = EmptyOperator(task_id='ingestion_completed')

    # ==================================================================
    # STAGE 2: TRANSFORMATION (retry 2 lần — Spark job có thể OOM)
    # ==================================================================
    transform_movies_task = PythonOperator(
        task_id = "transform_movie",
        python_callable= movies_run.run_all_movies_transformation,
        retries=2,
        retry_delay=timedelta(minutes=5),            # Chờ lâu hơn cho Spark giải phóng tài nguyên
    )

    transform_reviews_task = PythonOperator(
        task_id='transform_reviews_silver',
        python_callable=reviews_run.run_all_reviews_transformation,
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    merge_silver_task = PythonOperator(
        task_id = "merge_reviews_silver",
        python_callable= merge_reviews_silver.create_training_dataset,
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    end_of_transform = EmptyOperator(task_id="transform_completed")

    # ==================================================================
    # STAGE 3: ML INFERENCE (retry 2 lần — load model có thể lỗi I/O)
    # ==================================================================
    start_ml = EmptyOperator(task_id="start_ml")

    ml_task = PythonOperator(
        task_id = "ml_inference",
        python_callable= inference.predict_daily_reviews,
        op_kwargs={'target_date': '{{ ds }}'},
        retries=2,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(hours=1),        # Inference nhanh hơn training
    )

    end_ml = EmptyOperator(task_id='end_ml')

    # ==================================================================
    # STAGE 4: GOLD LAYER (retry 2 lần — DB có thể bị lock/timeout)
    # ==================================================================
    create_star_schema_task = SQLExecuteQueryOperator(
        task_id='create_star_schema_tables',
        conn_id="postgres_dwh",
        sql="sql/create_star_schema.sql",
        autocommit=True,
        split_statements=True,
        retries=2,
        retry_delay=timedelta(minutes=3),
    )

    end_db_task = EmptyOperator(task_id='end_db_task')

    gold_elt_task = PythonOperator(
        task_id='gold_layer_elt',
        python_callable=run_gold_layer,
        op_kwargs={'target_date': '{{ ds }}'},
        retries=2,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(hours=1),
    )

    # ==================================================================
    # ĐỊNH NGHĨA LUỒNG CHẠY
    # ==================================================================
    # Stage 1: Ingestion (IMDB + TMDB chạy song song)
    start_pipeline >> [ingest_imdb_task, ingest_tmdb_task] >> end_of_ingestion

    # Stage 2: Transformation (movies + reviews song song, merge chờ reviews xong)
    end_of_ingestion >> [transform_movies_task, transform_reviews_task]
    transform_reviews_task >> merge_silver_task
    [transform_movies_task, merge_silver_task] >> end_of_transform

    # Stage 3: ML Inference
    end_of_transform >> start_ml >> ml_task >> end_ml

    # Stage 4: Gold Layer
    end_ml >> create_star_schema_task >> end_db_task >> gold_elt_task
