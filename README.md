docker network create project-net
docker compose -f deployment/airflow/docker-compose.airflow.yaml --env-file deployment/airflow/.env up -d
docker compose -f deployment/spark/docker-compose.spark.yaml --env-file deployment/spark/.env up -d
docker compose -f deployment/minio/docker-compose.minio.yaml --env-file deployment/minio/.env up -d

docker exec spark-spark-master-1 /opt/spark/bin/spark-submit --master spark://spark-spark-master-1:7077 local:///opt/spark-apps/notebooks/2_silver/test.py

docker exec -i -t b2cb6820dd4a /bin/bash   

docker build . -t airflow/airflow:2.10.5
docker build . -t our-own-apache-spark