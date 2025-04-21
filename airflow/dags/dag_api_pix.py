import sys
import os
from datetime import datetime, date
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task

sys.path.append("/opt/airflow")

from scripts.extract_load.get_api import extract_api

default_args = {
    "owner": "mateus coppi",
    "depends_on_past": False,
}

with DAG(
    dag_id="extract_pix_data_from_api",
    start_date=datetime(2024, 4, 21),
    catchup=False,
    schedule_interval=None
):
    
    start = EmptyOperator(task_id="start_extract")
    
    @task()
    def extract_task():
        date_now = date.today().strftime("%Y%m")
        extract_api(date=date_now)

    end = EmptyOperator(task_id="end_extract")

    start >> extract_task() >> end
