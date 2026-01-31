from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("CheckHadoop").getOrCreate()
print("Hadoop Version: " + spark.sparkContext._gateway.jvm.org.apache.hadoop.util.VersionInfo.getVersion())
