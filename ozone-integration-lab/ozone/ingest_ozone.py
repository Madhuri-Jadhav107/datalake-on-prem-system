from pyspark.sql import SparkSession
import sys
import os

# --- Configuration ---
# Define the Iceberg Catalog pointing to Ozone
# We use a Hadoop catalog because it's simplest for file-system based storage like OFS without a separate Metastore service for this specific test
CATALOG_NAME = "ozone_iceberg"
WAREHOUSE_PATH = "ofs://om/vol1/bucket1/iceberg"

# Input/Output defaults
DEFAULT_INPUT = "/home/iceberg/local/sample.csv"
DEFAULT_TABLE = "test_table"

def create_spark_session():
    print("Initializing Spark Session with Ozone + Iceberg config...")
    spark = SparkSession.builder \
        .appName("OzoneIcebergIngest") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config(f"spark.sql.catalog.{CATALOG_NAME}", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.type", "hadoop") \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.warehouse", WAREHOUSE_PATH) \
        .config("spark.hadoop.fs.ofs.impl", "org.apache.hadoop.fs.ozone.OzoneFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.ofs.impl", "org.apache.hadoop.fs.ozone.OzFs") \
        .getOrCreate()
    
    # Set log level to reduce noise
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def main():
    # Parse args
    input_file = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_INPUT
    table_name = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_TABLE
    
    full_table_name = f"{CATALOG_NAME}.default.{table_name}"
    
    print(f"--- Configuration ---")
    print(f"Input File: {input_file}")
    print(f"Target Iceberg Table: {full_table_name}")
    print(f"Warehouse Location: {WAREHOUSE_PATH}")
    print("---------------------")

    spark = create_spark_session()

    try:
        # 1. Read Data
        print(f"Reading CSV data from {input_file}...")
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_file)
        print("Data Schema:")
        df.printSchema()

        # 2. Write to Ozone (Iceberg)
        print(f"Writing data to {full_table_name}...")
        # using createOrReplace for idempotency during testing
        df.writeTo(full_table_name).createOrReplace()
        print("Write operation completed.")

        # 3. Verify
        print("Verifying data by reading back from Iceberg...")
        result_df = spark.read.table(full_table_name)
        count = result_df.count()
        print(f"Verification Successful! Table {full_table_name} contains {count} records.")
        result_df.show(5)

    except Exception as e:
        print(f"\n[ERROR] Ingestion failed: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
