from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from tasks.tasks_elt_001_breweries import bronze_extract_breweries, bronze_extract_metadata, ipynb_command
import os
import json

def read_and_log_gold_file():
    file_path = "/opt/airflow/spark/notebooks/3_gold/gold_001_breweries.json"  # Caminho atualizado para o arquivo JSON
    if os.path.exists(file_path):
        with open(file_path, 'r') as file:
            content = json.load(file)  # Carregar o conteúdo do arquivo JSON
            print(f"Conteúdo do arquivo {file_path}:\n{json.dumps(content, indent=2)}")  # Imprime o JSON de forma legível
    else:
        print(f"Arquivo {file_path} não encontrado.")

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
        bash_command=ipynb_command("silver_001_breweries")
    )

    task_gold_transform_breweries = BashOperator(
        task_id="gold_transform_breweries",
        bash_command=ipynb_command("gold_001_breweries")
    )

    task_read_and_log_gold_file = PythonOperator(
        task_id="read_and_log_gold_file",
        python_callable=read_and_log_gold_file
    )

    [task_bronze_extract_breweries, task_bronze_extract_metadata] >> task_silver_transform_breweries >> task_gold_transform_breweries >> task_read_and_log_gold_file
