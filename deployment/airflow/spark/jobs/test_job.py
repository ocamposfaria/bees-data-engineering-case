from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("WordCount").getOrCreate()

data = ["olá mundo", "mundo spark", "hello world", "hello hello"]
df = spark.createDataFrame(data, "string").toDF("linha")

words = df.selectExpr("explode(split(linha, ' ')) as palavra")
contagem = words.groupBy("palavra").count()

contagem.show()

spark.stop()
