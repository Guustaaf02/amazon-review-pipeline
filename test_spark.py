from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Test").getOrCreate()
df = spark.createDataFrame([
    ("Alice", 5),
    ("Bob", 10),
    ("Carol", 15)
], ["nome", "valor"])

df.show()
spark.stop()
