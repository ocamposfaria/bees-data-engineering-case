from pyspark.sql import SparkSession

# Criando a SparkSession com configs do MinIO
spark = SparkSession.builder \
    .appName("Read from MinIO S3A") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "ROOTUSER") \
    .config("spark.hadoop.fs.s3a.secret.key", "CHANGEME123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.defaultFS", "s3a://datalake") \
    .getOrCreate()

# Lê o arquivo Parquet do MinIO
df = spark.read.parquet("s3a://datalake/teste_parquet")
df.show()

# Lê o arquivo JSON do MinIO
df_json = spark.read.json("s3a://datalake/bronze/breweries.json")
df_json.show(truncate=False)
