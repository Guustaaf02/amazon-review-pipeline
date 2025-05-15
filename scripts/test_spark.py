from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Test").getOrCreate()
print("Sessão Spark criada com sucesso!")

df = spark.read.csv("data/raw/train.csv", header=False)
print(f"Linhas no CSV: {df.count()}")