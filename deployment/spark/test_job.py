from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Cria sessão Spark
spark = SparkSession.builder \
    .appName("TesteTransformacao") \
    .getOrCreate()

# Cria um DataFrame de exemplo
data = [("João", 28), ("Maria", 33), ("Pedro", 45)]
df = spark.createDataFrame(data, ["nome", "idade"])

# Aplica transformação
df_filtrado = df.filter(col("idade") > 30)

# Mostra o resultado
df_filtrado.show()

# Encerra sessão
spark.stop()
