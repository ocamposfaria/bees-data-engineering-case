import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from tasks.tasks_elt_001_breweries import (
    bronze_extract_breweries,
    bronze_extract_metadata,
    ipynb_command,
    publish_metadata
)

default_args = {
    "email": [os.getenv("AIRFLOW_ON_FAILED_EMAIL")],
    "email_on_failure": True,
    "email_on_success": False,
    "retries": 0
}

with DAG(
    dag_id="elt_001_breweries",
    start_date=datetime(2024, 9, 4),
    schedule_interval="0 9 * * *",
    catchup=False,
    tags=["brewery"],
    default_args=default_args,
) as dag:

    task_bronze_extract_breweries = PythonOperator(
        task_id="bronze_extract_breweries",
        python_callable=bronze_extract_breweries
    )

    task_bronze_extract_metadata = PythonOperator(
        task_id="bronze_extract_metadata",
        python_callable=bronze_extract_metadata
    )

    task_silver_transform_breweries = BashOperator(
        task_id="silver_transform_breweries",
        bash_command=ipynb_command(layer="2_silver", app_name="silver_001_breweries")
    )

    task_gold_transform_breweries = BashOperator(
        task_id="gold_transform_breweries",
        bash_command=ipynb_command(layer="3_gold", app_name="gold_001_breweries")
    )

    task_publish_metadata = PythonOperator(
        task_id="publish_metadata",
        python_callable=publish_metadata
    )

    [task_bronze_extract_breweries, task_bronze_extract_metadata] >> task_silver_transform_breweries
    task_silver_transform_breweries >> task_gold_transform_breweries
    task_gold_transform_breweries >> task_publish_metadata
