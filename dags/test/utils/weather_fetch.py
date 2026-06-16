from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

# ให้ Airflow หา script เจอ
sys.path.append("/opt/airflow/scripts")

from kafka_producer import main   # เดี๋ยวเราจะทำให้ producer มี main()

with DAG(
    dag_id="fetch_weather_etl",
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",
    catchup=False
) as dag:

    run_etl = PythonOperator(
        task_id="run_producer",
        python_callable=main
    )

    run_etl