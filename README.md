
![alt text](image.png)

docker network create project-net
docker compose -f minio/docker-compose.minio.yaml --env-file minio/.env up --build -d
docker compose -f airflow/docker-compose.airflow.yaml up --build -d
docker compose -f spark/docker-compose.spark.yaml up --build -d
docker compose -f streamlit/docker-compose.streamlit.yaml up --build -d
