from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Test").getOrCreate()
print(f"Count: {spark.range(10).count()}")
