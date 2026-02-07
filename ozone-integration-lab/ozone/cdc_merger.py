from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# --- Configuration ---
CATALOG_NAME = "iceberg_hive"
DB_NAME = "trino_db"
TABLE_NAME = "customers"
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "source_db.public.customers" # Debezium default topic format

def create_spark_session():
    return SparkSession.builder \
        .appName("CDC-Iceberg-Merger") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config(f"spark.sql.catalog.{CATALOG_NAME}", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.type", "hive") \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.uri", "thrift://hive-metastore:9083") \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.warehouse", "s3a://madhuri-bucket/iceberg") \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.hadoop.fs.s3a.endpoint", "http://s3g:9878") \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.hadoop.fs.s3a.access.key", "anyID") \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.hadoop.fs.s3a.secret.key", "anySecret") \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.hadoop.fs.s3a.path.style.access", "true") \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.defaultCatalog", CATALOG_NAME) \
        .getOrCreate()

# Schema for Debezium JSON payload
# Note: This should match your source table columns
schema = StructType([
    StructField("payload", StructType([
        StructField("after", StructType([
            StructField("id", IntegerType()),
            StructField("name", StringType()),
            StructField("email", StringType()),
            StructField("city", StringType())
        ])),
        StructField("op", StringType())
    ]))
])

def process_batch(batch_df, batch_id):
    if batch_df.count() == 0:
        return

    print(f"Processing batch {batch_id} with {batch_df.count()} events...")
    
    # Register batch as temp view
    batch_df.createOrReplaceTempView("updates")
    
    # Perform MERGE INTO
    # We handle 'c' (create) and 'u' (update) by upserting
    # For 'd' (delete), you would add a DELETE logic
    spark.sql(f"""
        MERGE INTO {CATALOG_NAME}.{DB_NAME}.{TABLE_NAME} t
        USING updates s
        ON t.id = s.id
        WHEN MATCHED AND s.op = 'd' THEN DELETE
        WHEN MATCHED THEN UPDATE SET t.name = s.name, t.email = s.email, t.city = s.city
        WHEN NOT MATCHED AND s.op != 'd' THEN INSERT (id, name, email, city) VALUES (s.id, s.name, s.email, s.city)
    """)

if __name__ == "__main__":
    spark = create_spark_session()
    
    # Read from Kafka
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse JSON
    parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.payload.after.*", "data.payload.op") \
        .filter("id IS NOT NULL") # Skip deletes where after is null or malformed data

    # Write to Iceberg via foreachBatch
    query = parsed_df.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", "/tmp/checkpoints/cdc_merger") \
        .start()

    query.awaitTermination()
