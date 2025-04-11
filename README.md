
docker network create project-net

docker build deployment/airflow/ -t airflow/airflow:2.10.5
docker build deployment/spark/ -t our-own-apache-spark

docker compose -f deployment/minio/docker-compose.minio.yaml --env-file deployment/minio/.env up -d
docker compose -f deployment/airflow/docker-compose.airflow.yaml up -d
docker compose -f deployment/spark/docker-compose.spark.yaml up -d


docker exec spark-spark-master-1 /opt/spark/bin/spark-submit --master spark://spark-spark-master-1:7077 local:///opt/spark-apps/notebooks/2_silver/test.py

docker exec -i -t b2cb6820dd4a /bin/bash   

