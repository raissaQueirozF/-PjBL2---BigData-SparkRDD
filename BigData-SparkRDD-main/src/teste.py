from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TesteSpark") \
    .master("local[*]") \
    .getOrCreate()

print("Spark funcionando 😄🔥")

spark.stop()