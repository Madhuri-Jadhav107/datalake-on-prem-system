from pyspark.sql import SparkSession
import sys
import os

# --- Configuration ---
# Use 'hive' type catalog so Trino can see it via HMS
CATALOG_NAME = "ozone_hive_catalog"
# Use s3 scheme which Trino natively supports
WAREHOUSE_PATH = "s3://bucket1/iceberg"

# Input/Output defaults
DEFAULT_INPUT = "/home/iceberg/local/sample.csv"
DEFAULT_TABLE = "trino_table"

def create_spark_session():
    print("Initializing Spark Session with Trino-Compatible (Hive+S3A) config...")
    spark = SparkSession.builder \
        .appName("OzoneTrinoIngest") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config(f"spark.sql.catalog.{CATALOG_NAME}", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.type", "hive") \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.uri", "thrift://hive-metastore:9083") \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.warehouse", WAREHOUSE_PATH) \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.hadoop.conf.hive.metastore.warehouse.dir", WAREHOUSE_PATH) \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.hadoop.fs.s3a.endpoint", "http://s3g:9878") \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.hadoop.fs.s3a.access.key", "anyID") \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.hadoop.fs.s3a.secret.key", "anySecret") \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.hadoop.fs.s3a.path.style.access", "true") \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.hadoop.fs.s3a.directory.marker.retention", "keep") \
        .config("spark.sql.defaultCatalog", CATALOG_NAME) \
        \
        .config("spark.hadoop.fs.s3a.endpoint", "http://s3g:9878") \
        .config("spark.hadoop.fs.s3a.access.key", "anyID") \
        .config("spark.hadoop.fs.s3a.secret.key", "anySecret") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.directory.marker.retention", "keep") \
        .config("spark.hadoop.fs.s3a.fast.upload", "true") \
        .config("spark.hadoop.fs.s3a.retry.limit", "10") \
        .config("spark.hadoop.fs.s3a.retry.interval", "500ms") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("INFO")
    return spark

def main():
    input_file = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_INPUT
    
    # Adjust input file path to be local file (since we are using s3a defaultFS potentially? 
    # No, we didn't set defaultFS to s3a, so it defaults to file:// or whatever is configured, likely file:// in local mode)
    if not input_file.startswith("file://") and "://" not in input_file:
        input_file = f"file://{input_file}"
    
    table_name = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_TABLE
    
    spark = create_spark_session()

    try:
        db_name = "trino_db"
        full_table_name = f"{CATALOG_NAME}.{db_name}.{table_name}"
        
        print(f"--- Configuration ---")
        print(f"Input File: {input_file}")
        print(f"Target Iceberg Table: {full_table_name}")
        print(f"Warehouse Location: {WAREHOUSE_PATH}")
        print("---------------------")

        print(f"Creating database {db_name} if it doesn't exist...")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG_NAME}.{db_name} LOCATION '{WAREHOUSE_PATH}/{db_name}'")
        
        print(f"Reading CSV data from {input_file}...")
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_file)
        
        print(f"Writing data to {full_table_name}...")
        # Use createOrReplace to ensure we can overwrite existing tables
        df.writeTo(full_table_name).createOrReplace()
        print("Write operation completed.")

        print("Verifying data via Spark...")
        result_df = spark.read.table(full_table_name)
        count = result_df.count()
        print(f"Verification Successful! Table {full_table_name} contains {count} records.")
        result_df.show(5)

    except Exception as e:
        print("\n" + "!" * 60)
        print("INGESTION CRITICAL ERROR:")
        import traceback
        traceback.print_exc(file=sys.stdout)
        print("!" * 60 + "\n")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
