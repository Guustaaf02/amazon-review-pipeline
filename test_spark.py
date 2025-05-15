from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Test").getOrCreate()
<<<<<<< HEAD
df = spark.createDataFrame([
    ("Alice", 5),
    ("Bob", 10),
    ("Carol", 15)
], ["nome", "valor"])

df.show()
spark.stop()
=======
print("Sessão Spark criada com sucesso!")

df = spark.read.csv("data/raw/train.csv", header=False)
print(f"Linhas no CSV: {df.count()}")
>>>>>>> ccef164 (Adição das camadas silver e bronze)
