import boto3
import json
import logging
from botocore.exceptions import ClientError
import os

class MinioClient:
    def __init__(self):
        self.s3 = boto3.client(
            "s3",
            endpoint_url="http://minio:9000",
            aws_access_key_id=os.getenv("MINIO_ROOT_USER"),
            aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD")
        )

    def upload_json(self, bucket, key, data):
        try:
            self.s3.head_bucket(Bucket=bucket)
        except ClientError:
            self.s3.create_bucket(Bucket=bucket)

        self.s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(data),
            ContentType="application/json"
        )
        logging.info(f"Enviado para MinIO: {bucket}/{key}")
