import argparse
import os
import sys
import pyspark
from pyspark.sql import SparkSession

# Auto-detect Spark version to select correct Hudi bundle
spark_version = pyspark.__version__
major_minor = ".".join(spark_version.split(".")[:2])
print(f"Detected PySpark Version: {spark_version}")

if major_minor == '3.5':
    HUDI_VERSION = '0.15.0'
    SPARK_BUNDLE_VERSION = '3.5'
elif major_minor == '3.4':
    HUDI_VERSION = '0.14.0'
    SPARK_BUNDLE_VERSION = '3.4'
elif major_minor == '3.3':
    HUDI_VERSION = '0.14.0'
    SPARK_BUNDLE_VERSION = '3.3'
else:
    print(f"WARNING: Untested Spark version {spark_version}. Defaulting to configuration for 3.4")
    HUDI_VERSION = '0.14.0'
    SPARK_BUNDLE_VERSION = '3.4'

print(f"Using Hudi Version: {HUDI_VERSION} for Spark Bundle: {SPARK_BUNDLE_VERSION}")

SUBMIT_ARGS = f"--packages org.apache.hadoop:hadoop-aws:3.3.4,org.apache.hudi:hudi-spark{SPARK_BUNDLE_VERSION}-bundle_2.12:{HUDI_VERSION} pyspark-shell"
os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS
os.environ['PYSPARK_PYTHON'] = sys.executable

def create_spark_session():
    return SparkSession.builder \
        .appName("Hudi_CSV_Ingest") \
        .config('spark.executor.memory', '1g') \
        .config('spark.driver.memory', '1g') \
        .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
        .config('spark.sql.extensions', 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension') \
        .config('className', 'org.apache.hudi') \
        .config('spark.sql.hive.convertMetastoreParquet', 'false') \
        .getOrCreate()

def configure_s3(spark):
    # Use localhost ports for Spark running on host
    # MinIO is mapped to 9002 in this setup
    spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://localhost:9002")
    spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "minioadmin")
    spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "minioadmin")
    spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    spark._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
    spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

def ingest_csv(file_path, table_name, record_key, partition_field=None, precombine_field=None):
    spark = create_spark_session()
    configure_s3(spark)
    
    print(f"Reading CSV from: {file_path}")
    # Read CSV
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
    
    # Sanitize column names for Avro compatibility (replace spaces with underscores, remove special chars)
    new_columns = [c.strip().replace(" ", "_").replace(".", "").replace("/", "_").replace("(", "").replace(")", "") for c in df.columns]
    df = df.toDF(*new_columns)
    
    # Add a current timestamp column for Hudi precombine
    # Add a current timestamp column for Hudi precombine
    from pyspark.sql.functions import current_timestamp, lit
    df = df.withColumn("current_ts", current_timestamp().cast("string"))

    print("Sanitized Schema:")
    df.printSchema()
    
    # Path in MinIO
    path = f"s3a://warehouse/{table_name}"
    
    # Default precombine to current_ts if not provided
    actual_precombine = precombine_field if precombine_field else "current_ts"
    
    hudi_options = {
        'hoodie.table.name': table_name,
        'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
        'hoodie.datasource.write.operation': 'upsert',
        'hoodie.datasource.write.recordkey.field': record_key,
        'hoodie.datasource.write.precombine.field': actual_precombine,
        'hoodie.datasource.write.reconcile.schema': 'true',
        'hoodie.datasource.write.schema.evolution.enable': 'true',
        'hoodie.datasource.hive_sync.support_timestamp': 'true',
        
        "hoodie.enable.data.skipping": "false",
        "hoodie.metadata.enable": "false",
        "hoodie.datasource.hive_sync.metastore.uris": "thrift://localhost:9084", 
        "hoodie.datasource.hive_sync.mode": "hms",
        "hoodie.datasource.hive_sync.enable": "true",
        "hoodie.datasource.hive_sync.database": "default",
        "hoodie.datasource.hive_sync.table": table_name,
        "hoodie.datasource.hive_sync.username": "hive",
        "hoodie.datasource.hive_sync.password": "hive",
    }

    if partition_field:
        hudi_options["hoodie.datasource.write.partitionpath.field"] = partition_field
        hudi_options["hoodie.datasource.hive_sync.partition_extractor_class"] = "org.apache.hudi.hive.MultiPartKeysValueExtractor"
    else:
        hudi_options["hoodie.datasource.hive_sync.partition_extractor_class"] = "org.apache.hudi.hive.NonPartitionedExtractor"
        hudi_options["hoodie.datasource.write.keygenerator.class"] = "org.apache.hudi.keygen.NonpartitionedKeyGenerator"

    # Precombine is already set above
    # if precombine_field:
    #     hudi_options['hoodie.datasource.write.precombine.field'] = precombine_field

    # --- SCHEMA EVOLUTION HANDLING ---
    # To fix 'incompatible columns' error, we must insure the dataframe we write includes ALL columns 
    # that ever existed in the table, setting nulls for missing ones.
    
    try:
        # Check if table already exists in Hudi/Spark
        # We can try reading a single record to get the schema
        existing_df = spark.read.format("hudi").load(path)
        existing_schema = existing_df.schema
        
        print("Existing Table Schema Found. Merging schemas...")
        
        # Add missing columns from existing table to new dataframe (set as null)
        for field in existing_schema.fields:
            if field.name not in df.columns:
                print(f"Adding missing column from history: {field.name}")
                df = df.withColumn(field.name, lit(None).cast(field.dataType))
                
        # Add missing columns from new dataframe to existing schema (logic handled by 'mergeSchema' option usually, 
        # but explicit cast helps).
        
        # Align columns order
        # We prefer the new schema's order but ensuring all old columns are present
        # Actually, for Hudi append, it's safer to select columns in the order of the existing schema + new ones at end
        
        # Union list of columns
        all_cols = existing_df.columns + [c for c in df.columns if c not in existing_df.columns]
        
        # Select with casting to ensure types match existing if possible (simple cast)
        # For now just select by name to align order
        df = df.select(*all_cols)
        
    except Exception as e:
        # Table likely doesn't exist yet, which is fine
        print(f"No existing table found (or error reading it): {e}. Proceeding with new schema.")

    print(f"\nWriting to {path}...")
    
    df.write.format("hudi"). \
        options(**hudi_options). \
        mode("append"). \
        save(path)
    
    print("Ingestion complete.")
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest CSV into Hudi")
    parser.add_argument("--file", required=True, help="Path to local CSV file")
    parser.add_argument("--table", required=True, help="Target table name")
    parser.add_argument("--pkey", required=True, help="Primary key column name")
    parser.add_argument("--partition", help="Partition column name (optional)")
    parser.add_argument("--precombine", help="Precombine field (e.g. timestamp) for de-duplication (optional)")
    
    args = parser.parse_args()
    
    ingest_csv(args.file, args.table, args.pkey, args.partition, args.precombine)
