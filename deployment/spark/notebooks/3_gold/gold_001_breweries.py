#!/usr/bin/env python
# coding: utf-8

# ## Importações e Spark Session

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col
from pyspark.sql.functions import lower, regexp_replace

# carregando os jars necessários para se conectar ao MinIO (nosso S3)
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

# ## Lendo da camada silver e jogando na gold em parquet

# In[2]:


df = spark.read.parquet("s3a://datalake/2_silver/001_breweries/breweries/")

df.createOrReplaceTempView("breweries")

query = """
    SELECT 
        brewery_type,
        city,
        state_province,
        country,
        COUNT(*) AS brewery_count
    FROM breweries
    GROUP BY brewery_type, city, state_province, country
    ORDER BY brewery_count DESC
"""

gold_df = spark.sql(query)

gold_df.write.mode("overwrite").parquet("s3a://datalake/3_gold/001_breweries/breweries_by_type_location/")

gold_df.show()
