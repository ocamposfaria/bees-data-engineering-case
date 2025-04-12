
docker network create project-net
docker compose -f minio/docker-compose.minio.yaml --env-file minio/.env up --build -d
docker compose -f airflow/docker-compose.airflow.yaml up --build -d
docker compose -f spark/docker-compose.spark.yaml up --build -d
docker compose -f streamlit/docker-compose.streamlit.yaml up --build -d


docker exec spark-spark-master-1 /opt/spark/bin/spark-submit --master spark://spark-spark-master-1:7077 local:///opt/spark-apps/notebooks/2_silver/test.py

docker exec -i -t b2cb6820dd4a /bin/bash   
![alt text](image.png)