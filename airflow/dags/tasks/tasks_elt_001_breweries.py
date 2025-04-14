from classes.breweries import Breweries
from classes.minio import MinioClient
import logging
import os
import shutil

def bronze_extract_breweries():
    b = Breweries()
    data = b.get_all_breweries()
    logging.info(f"Extraído {len(data)} breweries.")
    MinioClient().upload_json("datalake", "1_bronze/001_breweries/breweries.json", data)

def bronze_extract_metadata():
    b = Breweries()
    metadata = b.get_breweries_metadata()
    logging.info(f"Metadados extraídos: {metadata}")
    MinioClient().upload_json("datalake", "1_bronze/001_breweries/metadata.json", metadata)

def ipynb_command(layer: str, app_name: str) -> str:
    ipynb_path = f"/opt/spark-apps/{layer}/{app_name}.ipynb"
    py_path = f"/opt/spark-apps/{layer}/{app_name}.py"
    return (
        f'docker exec spark-spark-master-1 bash -c '
        f'"jupyter nbconvert --to script {ipynb_path} && '
        f'/opt/spark/bin/spark-submit --master spark://spark-spark-master-1:7077 local://{py_path}; '
        f'status=$?; rm {py_path}; exit $status"'
    )

def publish_metadata():
    # isso aqui seria ajustado para capturar os metadados de todas as tabelas da camada gold
    # por enquanto ele só pega do pipeline 001_breweries
    source_path = "/opt/airflow/spark/notebooks/3_gold/gold_001_breweries.json"
    destination_path = "/opt/airflow/streamlit/gold_metadata/gold_001_breweries.json"

    if os.path.exists(source_path):
        os.makedirs(os.path.dirname(destination_path), exist_ok=True)
        shutil.copyfile(source_path, destination_path)
        print(f"Arquivo copiado para {destination_path}")
    else:
        print(f"Arquivo de origem {source_path} não encontrado.")

def fail_task():
    raise Exception("Falha proposital para testar envio de e-mail.")
