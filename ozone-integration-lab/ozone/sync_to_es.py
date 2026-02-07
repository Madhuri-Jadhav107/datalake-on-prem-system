import sys
from pyspark.sql import SparkSession

if len(sys.argv) < 2:
    print("Usage: sync_to_es.py <table_name>")
    sys.exit(1)

table_name = sys.argv[1]

# Initialize Spark Session with Iceberg and ES configs
spark = SparkSession.builder \
    .appName(f"Sync-to-ES-{table_name}") \
    .config("spark.hadoop.fs.s3a.access.key", "anyID") \
    .config("spark.hadoop.fs.s3a.secret.key", "anySecret") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://s3g:9878") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hive") \
    .config("spark.sql.catalog.iceberg.uri", "thrift://hive-metastore:9083") \
    .config("spark.sql.catalog.iceberg.warehouse", "s3a://madhuri-bucket/") \
    .getOrCreate()

print(f"Reading data from {table_name}...")
df = spark.table(f"iceberg.trino_db.{table_name}")

# In a real enterprise setup, we would filter for a "last_sync" timestamp here
# df = df.filter("updated_at > last_sync_timestamp")

print(f"Indexing {table_name} into Elasticsearch...")

# Writing to Elasticsearch
# Note: We use the ES-Spark connector for high-volume indexing
# For this PoC/Demo, we use the standard spark-es approach
df.write \
    .format("org.elasticsearch.spark.sql") \
    .option("es.nodes", "elasticsearch") \
    .option("es.port", "9200") \
    .option("es.resource", f"{table_name}") \
    .option("es.index.auto.create", "true") \
    .option("es.nodes.wan.only", "true") \
    .mode("overwrite") \
    .save()

print(f"Successfully indexed {table_name} into Elasticsearch!")
spark.stop()
