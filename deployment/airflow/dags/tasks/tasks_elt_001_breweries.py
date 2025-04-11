from classes.breweries import Breweries
from classes.minio import MinioClient
import logging

def ipynb_command(app_name: str) -> str:
    ipynb_path = f"/opt/spark-apps/2_silver/{app_name}.ipynb"
    py_path = f"/opt/spark-apps/2_silver/{app_name}.py"
    return (
        f'docker exec spark-spark-master-1 bash -c '
        f'"jupyter nbconvert --to script {ipynb_path} && '
        f'/opt/spark/bin/spark-submit --master spark://spark-spark-master-1:7077 local://{py_path}"'
    )

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
