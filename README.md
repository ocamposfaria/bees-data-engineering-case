


# BEES & ABInBev Data Engineering Challenge
![alt text](image-5.png)

<div align="center">

  <img src="https://img.shields.io/badge/Python-Language-blue?logo=python" alt="Python"/>
  <img src="https://img.shields.io/badge/Airflow-Orchestration-brightgreen?logo=apache-airflow" alt="Apache Airflow"/>
  <img src="https://img.shields.io/badge/Spark-Processing-orange?logo=apache-spark" alt="Apache Spark"/>
  <img src="https://img.shields.io/badge/MinIO-Object_Storage-red?logo=minio" alt="MinIO"/>
  <img src="https://img.shields.io/badge/Streamlit-Chat-ff4b4b?logo=streamlit" alt="Streamlit"/>
  <img src="https://img.shields.io/badge/OpenAI-API-black?logo=openai" alt="OpenAI"/>
    <img src="https://img.shields.io/badge/Docker-Compose-8e44ad?logo=docker" alt="Docker"/>

</div>


## Descrição do Projeto

Este projeto foi desenvolvido como parte do desafio técnico de Engenharia de Dados da BEES & ABInBev. Seu principal objetivo é demonstrar a capacidade de construir um pipeline de dados moderno, robusto e modular, utilizando a arquitetura medallion (bronze, silver e gold), desde a ingestão até a visualização interativa dos dados.

### Contextualização

A partir da API pública [Open Brewery DB](https://www.openbrewerydb.org/), que contém dados de cervejarias norte-americanas, os dados são extraídos, armazenados em um data lake, transformados e posteriormente agregados, seguindo boas práticas de engenharia de dados.

### Principais Funcionalidades

- **Extração de dados**
  - Coleta automatizada via Python `requests`
  - Orquestração e monitoramento do pipeline com Airflow
  - Retries e/ou envio de e-mails em caso de falhas

- **Armazenamento em Data Lake (MinIO)**
  - Camada Bronze: dados brutos
  - Camada Silver: dados em Parquet, limpos e particionados por localização
  - Camada Gold: dados agregados por tipo e estado

- **Transformações com PySpark em Jupyter Notebooks**
  - Limpeza, particionamento e agregação
  - Organização em notebooks para cada camada

- **Visualização dos dados com Streamlit e ChatGPT**
  - Assistente de IA via interface de chat com Streamlit
  - Integração com a API da OpenAI 

- **Containerização com Docker**
  - Serviços organizados via Docker Compose
  - Facilidade de reprodutibilidade local

## Pré-requisitos

Antes de rodar o projeto, verifique se você possui:

- **Docker** instalado (e Docker Compose):

- **Requisitos mínimos de hardware**:
  - CPU: 4 núcleos
  - Memória RAM: 12 GB (idealmente 16 GB)

## Como executar o projeto?

```bash
docker network create project-net
docker compose -f minio/docker-compose.minio.yaml --env-file minio/.env up --build -d
docker compose -f airflow/docker-compose.airflow.yaml up --build -d
docker compose -f spark/docker-compose.spark.yaml up --build -d
docker compose -f streamlit/docker-compose.streamlit.yaml up --build -d
```

## Estrutura de pastas
```
.
├── airflow/
│   ├── dags/
│   │   ├── classes/
│   │   ├── tasks/
│   │   └── elt_001_breweries.py
│   ├── Dockerfile
│   ├── requirements.txt
│   └── docker-compose.airflow.yaml
│
├── minio/
│   ├── .env
│   └── docker-compose.minio.yaml
│
├── spark/
│   ├── notebooks/
│   │   ├── 1-bronze/
│   │   ├── 2-silver/
│   │   └── 3-gold/
│   ├── Dockerfile
│   ├── start-spark.sh
│   └── docker-compose.spark.yaml
│
├── streamlit/
│   ├── gold-metadata/
│   ├── main.py
│   ├── requirements.txt
│   ├── Dockerfile
│   └── docker-compose.streamlit.yaml
└── images/

```
## Preview em funcionamento
### Airflow
![alt text](image-2.png)
### Streamlit AI Assistant
![alt text](image-3.png)



## Licença

Este projeto está licenciado sob a Licença MIT.



## Contato

Feito com ☕ por [**João Pedro Campos Faria**](https://www.linkedin.com/in/ocamposfaria)  
<p align="left">
  <img src="https://media.giphy.com/media/JIX9t2j0ZTN9S/giphy.gif" alt="Gato digitando" width="200"/>
</p>
