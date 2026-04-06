import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


AIRFLOW_HOME = Path(__file__).resolve().parent.parent
print(AIRFLOW_HOME)
if str(AIRFLOW_HOME) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_HOME))
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=7),
}

with DAG(
    dag_id="test_postgres_connection",
    default_args = default_args,
    schedule_interval='@daily',
    start_date=datetime(2026, 4, 3),
    catchup=False,
    tags=['test-connection']
) as dag:

    test_conn = SQLExecuteQueryOperator(
        task_id="test_connection",
        conn_id="postgres_dwh",
        sql="SELECT 1;"
    )