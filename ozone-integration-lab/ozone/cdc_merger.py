from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# --- Configuration ---
CATALOG_NAME = "iceberg_hive"
DB_NAME = "trino_db"
TABLE_NAME = "cdc_customers"
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
        .config(f"spark.sql.catalog.{CATALOG_NAME}.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

# Schema for Debezium JSON payload
# Note: This should match your source table columns
schema = StructType([
    StructField("payload", StructType([
        StructField("before", StructType([
            StructField("id", IntegerType()),
            StructField("name", StringType()),
            StructField("email", StringType()),
            StructField("city", StringType())
        ])),
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
    
    # Register batch as temp view in the batch-specific session
    batch_df.createOrReplaceTempView("updates")
    
    # Perform MERGE INTO using the batch-specific session
    # We use 'spark_catalog.default.updates' to ensure we don't look into the Iceberg catalog for the temp view
    batch_df.sparkSession.sql(f"""
        MERGE INTO {CATALOG_NAME}.{DB_NAME}.{TABLE_NAME} t
        USING updates s
        ON t.id = s.id
        WHEN MATCHED AND s.op = 'd' THEN DELETE
        WHEN MATCHED THEN UPDATE SET t.name = s.name, t.email = s.email, t.city = s.city
        WHEN NOT MATCHED AND s.op != 'd' THEN INSERT (id, name, email, city) VALUES (s.id, s.name, s.email, s.city)
    """)

if __name__ == "__main__":
    spark = create_spark_session()
    
    # Create table if not exists with correct schema
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG_NAME}.{DB_NAME}.{TABLE_NAME} (
            id INT,
            name STRING,
            email STRING,
            city STRING
        ) USING iceberg
    """)

    # Read from Kafka
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse JSON and handle Debezium 'before'/'after' for ID
    parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select(
            col("data.payload.before.id").alias("before_id"),
            col("data.payload.after.id").alias("after_id"),
            col("data.payload.after.name").alias("name"),
            col("data.payload.after.email").alias("email"),
            col("data.payload.after.city").alias("city"),
            col("data.payload.op").alias("op")
        ) \
        .selectExpr(
            "coalesce(after_id, before_id) as id",
            "name", "email", "city", "op"
        )

    # Write to Iceberg via foreachBatch
    query = parsed_df.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", "/tmp/checkpoints/cdc_merger") \
        .start()

    query.awaitTermination()
