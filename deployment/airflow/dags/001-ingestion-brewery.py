from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from classes.breweries import extract_breweries, extract_metadata
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='001-ingestion-brewery',
    default_args=default_args,
    description='Extrai dados da Open Brewery DB',
    schedule_interval='0 2 * * *',
    catchup=False,
    tags=['brewery', 'api']
) as dag:

    task_extract_breweries = PythonOperator(
        task_id='extract_breweries',
        python_callable=extract_breweries
    )

    task_extract_metadata = PythonOperator(
        task_id='extract_metadata',
        python_callable=extract_metadata
    )

    task_spark_transformation = BashOperator(
        task_id='run_spark_transformation',
        bash_command='docker exec spark spark-submit --conf spark.jars.ivy=/tmp/.ivy2 /opt/spark/test_job.py'
    )

    task_extract_breweries
    task_extract_metadata
    task_spark_transformation
