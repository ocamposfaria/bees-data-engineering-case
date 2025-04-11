import requests
import logging
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

class Breweries:
    def __init__(self):
        self.BREWERIES_URL = "https://api.openbrewerydb.org/v1/breweries"
        self.META_URL = "https://api.openbrewerydb.org/v1/breweries/meta"
        self.PER_PAGE = 200
        self.MAX_RETRIES = 3
        self.TIMEOUT = 10

    def get_all_breweries(self):
        all_breweries = []
        page = 1
        logging.info("Iniciando extração de dados da Open Brewery API...")

        while True:
            url = f"{self.BREWERIES_URL}?per_page={self.PER_PAGE}&page={page}"
            logging.info(f"Solicitando página {page}...")

            for attempt in range(1, self.MAX_RETRIES + 1):
                try:
                    response = requests.get(url, timeout=self.TIMEOUT)
                    response.raise_for_status()
                    data = response.json()
                    break  
                except requests.RequestException as e:
                    logging.warning(f"Tentativa {attempt}/{self.MAX_RETRIES} falhou na página {page}: {e}")
                    if attempt == self.MAX_RETRIES:
                        logging.error(f"Erro definitivo na página {page} após {self.MAX_RETRIES} tentativas.")
                        raise RuntimeError(f"Falha ao extrair a página {page} da API.") from e
                    time.sleep(20) 

            if not data:
                logging.info("Sem mais dados. Encerrando extração.")
                break

            all_breweries.extend(data)
            logging.info(f"Página {page} coletada - Total acumulado: {len(all_breweries)}")
            page += 1
            time.sleep(0.2)

        logging.info(f"Extração finalizada com sucesso. Total de breweries: {len(all_breweries)}")
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
