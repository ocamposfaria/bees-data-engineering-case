import requests
import time
import logging
import boto3
import json
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

class Breweries:
    def __init__(self):
        self.BREWERIES_URL = "https://api.openbrewerydb.org/v1/breweries"
        self.META_URL = "https://api.openbrewerydb.org/v1/breweries/meta"
        self.PER_PAGE = 200

    def get_all_breweries(self):
        all_breweries = []
        page = 1
        logging.info("Starting data extraction from Open Brewery API...")

        while True:
            url = f"{self.BREWERIES_URL}?per_page={self.PER_PAGE}&page={page}"
            logging.info(f"Requesting page {page}...")

            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()
            except requests.RequestException as e:
                logging.error(f"Request failed on page {page}: {e}")
                break

            data = response.json()
            if not data:
                logging.info("No more data. Ending extraction.")
                break

            all_breweries.extend(data)
            logging.info(f"Page {page} collected - Total so far: {len(all_breweries)}")
            page += 1
            time.sleep(0.2)

        logging.info(f"Finished. Total breweries extracted: {len(all_breweries)}")
        return all_breweries

    def get_breweries_metadata(self):
        try:
            response = requests.get(self.META_URL, timeout=10)
            response.raise_for_status()
            metadata = response.json()
            logging.info("Metadados extraídos com sucesso.")
            return metadata
        except requests.RequestException as e:
            logging.error(f"Erro ao puxar metadados: {e}")
            return {}

# Inicializa cliente boto3
def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url="http://minio1:9000",
        aws_access_key_id="ROOTUSER",
        aws_secret_access_key="CHANGEME123"
    )

# Envia dados para MinIO
def upload_to_minio(bucket, key, data):
    s3 = get_s3_client()
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError:
        s3.create_bucket(Bucket=bucket)

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data),
        ContentType="application/json"
    )
    logging.info(f"Enviado para MinIO: {bucket}/{key}")


# Funções que serão chamadas na DAG
def extract_breweries():
    b = Breweries()
    data = b.get_all_breweries()
    logging.info(f"Extraído {len(data)} breweries.")
    upload_to_minio("datalake", "bronze/breweries.json", data)

def extract_metadata():
    b = Breweries()
    metadata = b.get_breweries_metadata()
    logging.info(f"Metadados: {metadata}")
    print('ENDPOINT MINIO')
    upload_to_minio("datalake", "bronze/metadata.json", metadata)

# para testes
if __name__ == "__main__":
    extract_breweries()
    extract_metadata()
