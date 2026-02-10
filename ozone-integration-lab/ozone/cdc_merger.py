from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, row_number, desc
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# --- Configuration ---
CATALOG_NAME = "iceberg_hive"
DB_NAME = "trino_db"
TABLE_NAME = "cdc_customers"
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC_PATTERN = ".*\\.public\\.customers|.*\\.inventory\\.customers"

def create_spark_session():
    return SparkSession.builder \
        .appName("CDC-Iceberg-Merger") \
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
        .config(f"spark.sql.catalog.{CATALOG_NAME}.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hive") \
        .config("spark.sql.catalog.spark_catalog.uri", "thrift://hive-metastore:9083") \
        .config("spark.sql.defaultCatalog", "spark_catalog") \
        .getOrCreate()

# Schema for Debezium JSON payload
# Note: This should match your source table columns
schema = StructType([
    StructField("payload", StructType([
        StructField("before", StructType([
            StructField("id", IntegerType()),
            StructField("name", StringType()),
            StructField("email", StringType()),
            StructField("city", StringType()),
            StructField("updated_at", StringType())  # Added updated_at
        ])),
        StructField("after", StructType([
            StructField("id", IntegerType()),
            StructField("name", StringType()),
            StructField("email", StringType()),
            StructField("city", StringType()),
            StructField("updated_at", StringType())  # Added updated_at
        ])),
        StructField("op", StringType())
    ]))
])

def process_batch(batch_df, batch_id):
    if batch_df.count() == 0:
        return

    print(f"Processing batch {batch_id} with {batch_df.count()} events...")
    
    # Identify unique sources in this batch
    sources = batch_df.select("topic").distinct().collect()
    
    for row in sources:
        topic = row.topic
        # Determine target table name based on topic
        # Postgres: source_db.public.customers -> cdc_customers
        # MySQL: mysql_source.inventory.customers -> cdc_mysql_customers
        target_table = "cdc_customers" if "source_db" in topic else "cdc_mysql_customers"
        
        print(f"Merging events from topic '{topic}' into table '{target_table}'")
        
        # Filter data for this specific source
        source_df = batch_df.filter(col("topic") == topic)
        
        # Deduplicate: Only keep the latest event for each record ID within this batch
        window_spec = Window.partitionBy("id").orderBy(desc("timestamp"))
        deduped_df = source_df.withColumn("rn", row_number().over(window_spec)) \
            .filter(col("rn") == 1) \
            .drop("rn", "timestamp", "topic")
        
        # Ensure target table exists (Auto-create)
        batch_df.sparkSession.sql(f"""
            CREATE TABLE IF NOT EXISTS {CATALOG_NAME}.{DB_NAME}.{target_table} (
                id INT,
                name STRING,
                email STRING,
                city STRING,
                updated_at STRING
            ) USING iceberg
        """)
        
        # Register deduplicated batch as temp view
        deduped_df.createOrReplaceTempView("updates")
        
        # Perform MERGE INTO
        batch_df.sparkSession.sql(f"""
            MERGE INTO {CATALOG_NAME}.{DB_NAME}.{target_table} t
            USING updates s
            ON t.id = s.id
            WHEN MATCHED AND s.op = 'd' THEN DELETE
            WHEN MATCHED THEN UPDATE SET t.name = s.name, t.email = s.email, t.city = s.city, t.updated_at = s.updated_at
            WHEN NOT MATCHED AND s.op != 'd' THEN INSERT (id, name, email, city, updated_at) VALUES (s.id, s.name, s.email, s.city, s.updated_at)
        """)

if __name__ == "__main__":
    spark = create_spark_session()
    
    # Create initial table if not exists (Postgres source)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG_NAME}.{DB_NAME}.cdc_customers (
            id INT,
            name STRING,
            email STRING,
            city STRING,
            updated_at STRING
        ) USING iceberg
    """)

    # Read from Kafka using subscribePattern for multi-source
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribePattern", KAFKA_TOPIC_PATTERN) \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse JSON and handle Debezium 'before'/'after' for ID
    # We include 'timestamp' and 'topic' from Kafka to route and deduplicate
    parsed_df = raw_df.select(
            col("timestamp"),
            col("topic"),
            from_json(col("value").cast("string"), schema).alias("data")
        ).select(
            col("timestamp"),
            col("topic"),
            col("data.payload.before.id").alias("before_id"),
            col("data.payload.after.id").alias("after_id"),
            col("data.payload.after.name").alias("name"),
            col("data.payload.after.email").alias("email"),
            col("data.payload.after.city").alias("city"),
            col("data.payload.after.updated_at").alias("updated_at"),
            col("data.payload.op").alias("op")
        ).selectExpr(
            "timestamp",
            "topic",
            "coalesce(after_id, before_id) as id",
            "name", "email", "city", "updated_at", "op"
        )

    # Write to Iceberg via foreachBatch
    query = parsed_df.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", "/tmp/checkpoints/cdc_merger") \
        .start()

    query.awaitTermination()
