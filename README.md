<div align="center">

  <img src="https://img.shields.io/badge/Python-Language-blue?logo=python" alt="Python"/>
  <img src="https://img.shields.io/badge/Airflow-Orchestration-brightgreen?logo=apache-airflow" alt="Apache Airflow"/>
  <img src="https://img.shields.io/badge/Spark-Processing-orange?logo=apache-spark" alt="Apache Spark"/>
  <img src="https://img.shields.io/badge/MinIO-Object_Storage-red?logo=minio" alt="MinIO"/>
  <img src="https://img.shields.io/badge/Streamlit-Chat-ff4b4b?logo=streamlit" alt="Streamlit"/>
  <img src="https://img.shields.io/badge/Docker-Compose-8e44ad?logo=docker" alt="Docker"/>

</div>

# BEES & ABInBev Data Engineering Challenge
![alt text](image-5.png)

## Como executar o projeto

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
│   ├── config/
│   ├── degs/
│   ├── logs/
│   ├── plugins/
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
│
├── image-2.png
├── image-3.png
└── image-4.png

```
## Preview
### Airflow
![alt text](image-2.png)
### AI Assistant
![alt text](image-3.png)
