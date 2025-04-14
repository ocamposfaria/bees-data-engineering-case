
# BEES & ABInBev Data Engineering Challenge
![alt text](images/image-3.png)

<div align="center">

  <img src="https://img.shields.io/badge/Python-Language-blue?logo=python" alt="Python"/>
  <img src="https://img.shields.io/badge/Airflow-Orchestration-brightgreen?logo=apache-airflow" alt="Apache Airflow"/>
  <img src="https://img.shields.io/badge/Spark-Processing-orange?logo=apache-spark" alt="Apache Spark"/>
  <img src="https://img.shields.io/badge/MinIO-Object_Storage-red?logo=minio" alt="MinIO"/>
  <img src="https://img.shields.io/badge/Streamlit-Chat-ff4b4b?logo=streamlit" alt="Streamlit"/>
  <img src="https://img.shields.io/badge/OpenAI-API-black?logo=openai" alt="OpenAI"/>
  <img src="https://img.shields.io/badge/Docker-Compose-8e44ad?logo=docker" alt="Docker"/>

</div>

## Project Description

This project was developed as part of the BEES & ABInBev Data Engineering technical challenge. Its main goal is to demonstrate the ability to build a modern, robust, and modular data pipeline using the medallion architecture (bronze, silver, and gold), from ingestion to interactive data visualization.

### Context

Using the public API [Open Brewery DB](https://www.openbrewerydb.org/), which contains data on American breweries, the data is extracted, stored in a data lake, transformed, and later aggregated following data engineering best practices.

### Key Features

- **Data Extraction**
  - Automated collection using Python `requests`
  - Pipeline orchestration and monitoring with Airflow
  - Retries and/or e-mail notifications in case of failure

- **Storage in Data Lake (MinIO)**
  - Bronze Layer: raw data
  - Silver Layer: cleaned and partitioned Parquet data by location
  - Gold Layer: data aggregated by type and state

- **Transformations with PySpark in Jupyter Notebooks**
  - Cleaning, partitioning, and aggregation
  - Organized in notebooks for each layer

- **Data Visualization with Streamlit and ChatGPT**
  - AI assistant via chat interface with Streamlit
  - Integration with the OpenAI API

- **Containerization with Docker**
  - Services organized using Docker Compose
  - Easy local reproducibility

## Prerequisites

Before running the project, make sure you have:

- **Docker** installed (and Docker Compose)

- **Minimum hardware requirements**:
  - CPU: 4 cores
  - RAM: 12 GB (ideally 16 GB)

## How to Run the Project

1. **Clone the repository:**

```bash
git clone https://github.com/ocamposfaria/bees-data-engineering-case.git
cd bees-data-engineering-case
```

2. **Set up environment variables:**

- Create your own environment configuration file by copying the example to a `config.env` file:

```bash
cp config-example.env config.env
```

- Then edit the `config.env` file and replace the placeholder values with your own.

- **If you received a `config.env` file via email as part of the recruitment process, place it directly in the root of the repository instead of creating a new one.**

3. **Create the shared Docker network:**

```bash
docker network create project-net
```

4. **Start the project services:**

```bash
docker compose -f airflow/docker-compose.airflow.yaml --env-file ./config.env up --build -d
docker compose -f minio/docker-compose.minio.yaml --env-file ./config.env up --build -d
docker compose -f spark/docker-compose.spark.yaml --env-file ./config.env up --build -d
docker compose -f streamlit/docker-compose.streamlit.yaml --env-file ./config.env up --build -d
```

5. **Access the services at the following URLs:**

- Airflow: [http://localhost:8080/home](http://localhost:8080/home)
- MinIO: [http://localhost:9001](http://localhost:9001)
- JupyterLab: [http://localhost:8888/lab](http://localhost:8888/lab)  
- Streamlit: [http://localhost:8501](http://localhost:8501)  
- Spark Master UI: [http://localhost:9090](http://localhost:9090)


## Folders Structure
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
## Live Preview
### Airflow
![alt text](images/image-2.png)
### Jupyter
![alt text](images/image-6.png)
### Streamlit AI Assistant
![alt text](images/image-33.png)

## Contact

Made with ☕ by [**João Pedro Campos Faria**](https://www.linkedin.com/in/ocamposfaria)  

<p align="left">
  <img src="https://media.giphy.com/media/JIX9t2j0ZTN9S/giphy.gif" alt="Typing cat" width="150"/>
</p>
 