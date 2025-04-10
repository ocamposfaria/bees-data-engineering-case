docker network create project-net
docker compose -f airflow/docker-compose.airflow.yaml --env-file airflow/.env up -d
docker compose -f spark/docker-compose.spark.yaml --env-file spark/.env up -d
docker compose -f minio/docker-compose.minio.yaml --env-file minio/.env up -d

docker exec spark-spark-master-1 /opt/spark/bin/spark-submit --master spark://spark-spark-master-1:7077 local:///opt/spark-apps/test_job.py

docker exec -i -t 6456f4111805 /bin/bash   

