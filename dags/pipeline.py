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
print(AIRFLOW_HOME)
if str(AIRFLOW_HOME) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_HOME))

from ingestion import imdb_run, tmdb_run
from transformation import merge_reviews_silver, movies_run, reviews_run
from nlp.src import Logistic_Regression
from gold.gold_run import run_gold_layer

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=7),
}
with DAG(
    dag_id = "movie_sentiment_full_pipeline",
    default_args = default_args,
    schedule_interval='@daily',
    start_date=datetime(2026, 4, 3),
    catchup=False,
    tags=['movie', 'sentiment', 'spark', 'nlp'],
) as dag:
    
    # Stage 1: Ingestion 
    start_pipeline = EmptyOperator(task_id='start_pipeline')
    ingest_imdb_task = PythonOperator(
        task_id = "ingest_imdb_data",
        python_callable = imdb_run.main,
        op_kwargs={
            "movie_limit": 100,
            "review_per_movie": 2000,
            "max_worker": 15
        }
    )
    ingest_tmdb_task = PythonOperator(
        task_id = "ingest_tmdb_data",
        python_callable = tmdb_run.main,
        op_kwargs={
            "movie_limit": 100,
            "review_per_movie": 2000,
            "max_worker": 5
        }
    )
    end_of_ingestion = EmptyOperator(task_id='ingestion_completed')

    # Stage 2: Transformation 
    start_transform = EmptyOperator(task_id="start_transform")
    transform_movies_task = PythonOperator(
        task_id = "transform_movie",
        python_callable= movies_run.run_all_movies_transformation,
    )
    transform_reviews_task = PythonOperator(
        task_id='transform_reviews_silver',
        python_callable=reviews_run.run_all_reviews_transformation,
    )
    merge_silver_task = PythonOperator(
        task_id = "merge_reviews_silver",
        python_callable= merge_reviews_silver.create_training_dataset
    )
    end_of_transform = EmptyOperator(task_id="transform_completed")

    # Stage 3: Machine Learning
    # Do file Logistic_Regression chạy .ipynb nên phải dùng Bash
    start_ml = EmptyOperator(task_id="start_ml")
    ml_task = PythonOperator(
        task_id = "ml_task",
        python_callable= Logistic_Regression.train_sentiment_model
    )
    end_ml = EmptyOperator(task_id='end_ml')
    #Stage 4: Gold 
    create_star_schema_task = SQLExecuteQueryOperator(
        task_id='create_star_schema_tables',
        conn_id="postgres_dwh",
        sql="sql/create_star_schema.sql",
        autocommit=True, # Đảm bảo lệnh CREATE TABLE được commit ngay lập tức
        split_statements=True 
    )
    end_db_task = EmptyOperator(task_id='end_db_task')
    gold_elt_task = PythonOperator(
        task_id='gold_layer_elt',
        python_callable=run_gold_layer,
        op_kwargs={'target_date': '{{ ds }}'}     
    )
    # Luồng stage 1
    start_pipeline >> [ingest_imdb_task, ingest_tmdb_task] >> end_of_ingestion
    
    
    # Luồng stage 2
    # Biến đổi phim và review có thể chạy song song vì chúng ghi ra các folder khác nhau
    end_of_ingestion >> [transform_movies_task, transform_reviews_task]
    # Bắt buộc: Việc gộp file NLP chỉ chạy khi đã clean reviews xong
    transform_reviews_task >> merge_silver_task
    # Stage 2 (Chờ cả xử lý phim và gộp data NLP xong)
    [transform_movies_task, merge_silver_task] >> end_of_transform
    # Luồng stage 3
    end_of_transform >> start_ml >> ml_task
    ml_task >> end_ml
    # Luồng stage 4
    end_ml >> create_star_schema_task >> end_db_task
    end_db_task >> gold_elt_task