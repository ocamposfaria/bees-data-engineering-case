from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from tasks.tasks_elt_001_breweries import bronze_extract_breweries, bronze_extract_metadata


with DAG(
    dag_id="elt_001_breweries",
    start_date=datetime(2024, 9, 4),
    schedule_interval="0 9 * * *",
    catchup=False,
    tags=["brewery"],
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
        bash_command=(
            "docker exec spark-spark-master-1 bash -c \""
            "jupyter nbconvert --to script /opt/spark-apps/2_silver/silver_001_breweries.ipynb && "
            "/opt/spark/bin/spark-submit "
            "--master spark://spark-spark-master-1:7077 "
            "local:///opt/spark-apps/2_silver/silver_001_breweries.py\""
        )
    )

    [task_bronze_extract_breweries, task_bronze_extract_metadata]>>task_silver_transform_breweries
