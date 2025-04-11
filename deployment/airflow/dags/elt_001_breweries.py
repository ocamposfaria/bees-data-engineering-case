from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from tasks.tasks_elt_001_breweries import extract_breweries, extract_metadata
import papermill

with DAG(
    dag_id="elt_001_breweries",
    start_date=datetime(2024, 9, 4),
    schedule_interval="0 9 * * *",
    catchup=False,
    tags=["brewery"],
) as dag:

    task_extract_breweries = PythonOperator(
        task_id="extract_breweries",
        python_callable=extract_breweries
    )

    task_extract_metadata = PythonOperator(
        task_id="extract_metadata",
        python_callable=extract_metadata
    )

    task_extract_breweries
    task_extract_metadata
