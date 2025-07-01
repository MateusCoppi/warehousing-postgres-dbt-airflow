import sys
from datetime import datetime, date
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

sys.path.append("/opt/airflow")

from scripts.etl.get_api import extract_api

default_args = {
    "owner": "mateus coppi",
    "depends_on_past": False,
}

with DAG(
    dag_id="extract_pix_data_from_api",
    start_date=datetime(2025, 6, 1),
    catchup=True,
    schedule_interval="@daily"
):
    
    start = EmptyOperator(task_id="start_extract")
    
    @task()
    def extract_task():
        date_now = date.today().strftime("%Y%m")
        extract_api(date=date_now)

    end = EmptyOperator(task_id="end_extract")

    trigger_dbt_transformations = TriggerDagRunOperator(
        task_id='dbt_transformations_trigger',
        trigger_dag_id='dbt_data_transform'
    )

    start >> extract_task() >> end >> trigger_dbt_transformations
