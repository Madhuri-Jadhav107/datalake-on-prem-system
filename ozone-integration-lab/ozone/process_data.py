from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("OzoneIcebergTest") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "/home/iceberg/warehouse/local") \
    .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.demo.type", "hadoop") \
    .config("spark.sql.catalog.demo.warehouse", "/home/iceberg/warehouse/demo") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Read CSV from local mount
csv_path = "file:///home/iceberg/local/sample.csv"
df = spark.read.option("header", "true").csv(csv_path)

# Write to Iceberg table in the 'local' catalog
df.writeTo("local.db.sample_table_local").createOrReplace()

# Verify
print("Iceberg Table Output:")
spark.sql("SELECT * FROM local.db.sample_table_local").show()

