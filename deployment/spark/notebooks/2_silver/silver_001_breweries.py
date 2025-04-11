#!/usr/bin/env python
# coding: utf-8

# ## Imports e Spark Session

# In[ ]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col
from pyspark.sql.functions import lower, regexp_replace

# Display fix for notebooks
# from IPython.core.display import HTML
# display(HTML("<style>pre { white-space: pre !important; }</style>"))

# Create Spark session with MinIO (S3A)
spark = SparkSession.builder \
    .appName("Read from MinIO S3A and Write Partitioned") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "ROOTUSER") \
    .config("spark.hadoop.fs.s3a.secret.key", "CHANGEME123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.ui.showConsoleProgress", "true") \
    .getOrCreate()

# ## Lendo da camada /bronze/ e jogando na /silver/ em parquet

# In[2]:


df = spark.read.json("s3a://datalake/1_bronze/001_breweries/breweries.json")

df_cleaned = df.select([
    col(c).alias(c.lower().replace(" ", "_")) for c in df.columns
])

# adicionando timestamp da ingestão!
df_with_ts = df_cleaned.withColumn("ingestion_timestamp", current_timestamp())

# escrevendo particionado por estado
df_with_ts.write \
    .mode("overwrite") \
    .partitionBy("state") \
    .parquet("s3a://datalake/2_silver/001_breweries/breweries/")
