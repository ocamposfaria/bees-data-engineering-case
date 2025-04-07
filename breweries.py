import requests
import time
import json
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
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
