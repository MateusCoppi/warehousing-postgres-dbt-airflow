from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

DBT_PROJECT_PATH = "/opt/airflow/dbt"
DBT_EXECUTABLE = "/opt/airflow/dbt_env/bin/dbt"

default_args = {"owner": "airflow"}

with DAG(
    dag_id="dbt_data_transform",
    start_date=datetime(2025, 6, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
) as dag:

    start = EmptyOperator(task_id="start")
    
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir /opt/airflow/.dbt",
    )

    end = EmptyOperator(task_id="end")

start >> dbt_run >> end
