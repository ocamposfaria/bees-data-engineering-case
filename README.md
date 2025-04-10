/deployment/
docker network create project-net
docker compose -f deployment/airflow/docker-compose.airflow.yaml --env-file airflow/.env up -d
docker compose -f deployment/spark/docker-compose.spark.yaml --env-file deployment/spark/.env up -d
docker compose -f deployment/minio/docker-compose.minio.yaml --env-file deployment/minio/.env up -d

docker exec spark-spark-master-1 /opt/spark/bin/spark-submit --master spark://spark-spark-master-1:7077 local:///opt/spark-apps/test_job.py

docker exec -i -t 6456f4111805 /bin/bash   

