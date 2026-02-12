from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, row_number, desc
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# --- High-Throughput Configuration ---
CATALOG_NAME = "iceberg_hive"
DB_NAME = "trino_db"
TARGET_TABLE = "cdc_customers_optimized"
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC_PATTERN = ".*\\.public\\.customers|.*\\.inventory\\.customers"

# Optimized Partitioning & Shuffle
SHUFFLE_PARTITIONS = 64  # Match with Kafka partitions
BATCH_SIZE = 100000      # Max offsets per micro-batch

schemas = {
    "customers": StructType([
        StructField("payload", StructType([
            StructField("before", StructType([
                StructField("id", IntegerType()),
                StructField("name", StringType()),
                StructField("email", StringType()),
                StructField("city", StringType()),
                StructField("updated_at", StringType())
            ])),
            StructField("after", StructType([
                StructField("id", IntegerType()),
                StructField("name", StringType()),
                StructField("email", StringType()),
                StructField("city", StringType()),
                StructField("updated_at", StringType())
            ])),
            StructField("op", StringType())
        ]))
    ])
}

def create_spark_session():
    return SparkSession.builder \
        .appName("CDC-Iceberg-Merger-Optimized") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config(f"spark.sql.catalog.{CATALOG_NAME}", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.type", "hive") \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.uri", "thrift://hive-metastore:9083") \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.warehouse", "s3a://bucket1/iceberg") \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.hadoop.fs.s3a.endpoint", "http://s3g:9878") \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.hadoop.fs.s3a.access.key", "anyID") \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.hadoop.fs.s3a.secret.key", "anySecret") \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.hadoop.fs.s3a.path.style.access", "true") \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS) \
        .config("spark.streaming.backpressure.enabled", "true") \
        .config("spark.streaming.kafka.maxRatePerPartition", "5000") \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.write.distribution-mode", "hash") \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.write.metadata.delete-after-commit.enabled", "true") \
        .getOrCreate()

def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    print(f"🚀 Processing Optimized Batch {batch_id} | Rows: {batch_df.count()}")
    
    # Parse and Deduplicate
    parsed_df = batch_df.select(
        col("timestamp"),
        from_json(col("value").cast("string"), schemas["customers"]).alias("data")
    ).select(
        col("timestamp"),
        col("data.payload.before.id").alias("before_id"),
        col("data.payload.after.id").alias("after_id"),
        col("data.payload.after.name").alias("name"),
        col("data.payload.after.email").alias("email"),
        col("data.payload.after.city").alias("city"),
        col("data.payload.after.updated_at").alias("updated_at"),
        col("data.payload.op").alias("op")
    ).selectExpr(
        "coalesce(after_id, before_id) as id",
        "*"
    )

    # Latest record per ID for this batch
    deduped_df = parsed_df.withColumn("rn", row_number().over(Window.partitionBy("id").orderBy(desc("timestamp")))) \
        .filter(col("rn") == 1).drop("rn", "timestamp", "after_id", "before_id")

    deduped_df.createOrReplaceTempView("updates")
    
    # Atomic Merge with Iceberg
    batch_df.sparkSession.sql(f"""
        MERGE INTO {CATALOG_NAME}.{DB_NAME}.{TARGET_TABLE} t
        USING updates s
        ON t.id = s.id
        WHEN MATCHED AND s.op = 'd' THEN DELETE
        WHEN MATCHED THEN UPDATE SET 
            t.name = s.name, t.email = s.email, t.city = s.city, t.updated_at = s.updated_at
        WHEN NOT MATCHED AND s.op != 'd' THEN INSERT (id, name, email, city, updated_at) 
            VALUES (s.id, s.name, s.email, s.city, s.updated_at)
    """)

if __name__ == "__main__":
    spark = create_spark_session()
    
    # Initialize Table with Optimized Properties
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG_NAME}.{DB_NAME}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG_NAME}.{DB_NAME}.{TARGET_TABLE} (
            id INT, name STRING, email STRING, city STRING, updated_at STRING
        ) USING iceberg
        TBLPROPERTIES (
            'write.format.default'='parquet',
            'write.parquet.compression-codec'='snappy',
            'write.metadata.previous-versions-max'='10',
            'write.distribution-mode'='hash'
        )
    """)

    # Read from Kafka with High-Throughput Trigger
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribePattern", KAFKA_TOPIC_PATTERN) \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", BATCH_SIZE) \
        .load()

    query = raw_df.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", "/tmp/checkpoints/cdc_merger_optimized") \
        .trigger(processingTime="10 seconds") \
        .start()

    print(f"🔥 Optimized Merger Running for 1.6B Records. Target: {TARGET_TABLE}")
    query.awaitTermination()
