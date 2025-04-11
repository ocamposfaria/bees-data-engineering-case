from classes.breweries import Breweries
from classes.minio import MinioClient
import logging

def extract_breweries():
    b = Breweries()
    data = b.get_all_breweries()
    logging.info(f"Extraído {len(data)} breweries.")
    MinioClient().upload_json("datalake", "1_bronze/001_breweries/breweries.json", data)

def extract_metadata():
    b = Breweries()
    metadata = b.get_breweries_metadata()
    logging.info(f"Metadados extraídos: {metadata}")
    MinioClient().upload_json("datalake", "1_bronze/001_breweries/metadata.json", metadata)
