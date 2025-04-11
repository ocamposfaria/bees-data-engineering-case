from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='dag_dummy_teste',
    default_args=default_args,
    description='DAG dummy usando docker exec',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['teste', 'dummy']
) as dag:

    submit_job = BashOperator(
        task_id="submit_spark_job",
        bash_command=(
            "docker exec spark-spark-master-1 "
            "/opt/spark/bin/spark-submit "
            "--master spark://spark-spark-master-1:7077 "
            "local:///opt/spark-apps/2_silver/test.py"
        )
    )

    submit_job_2 = BashOperator(
        task_id="convert_and_submit_notebook",
        bash_command=(
            "docker exec spark-spark-master-1 bash -c \""
            "jupyter nbconvert --to script /opt/spark-apps/2_silver/silver_001_breweries.ipynb && "
            "/opt/spark/bin/spark-submit "
            "--master spark://spark-spark-master-1:7077 "
            "local:///opt/spark-apps/2_silver/silver_001_breweries.py\""
        )
    )

    submit_job_2
