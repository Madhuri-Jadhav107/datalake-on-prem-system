from pyspark.sql import SparkSession
import sys
import os

# --- Configuration ---
# Define the Iceberg Catalog pointing to Ozone
CATALOG_NAME = "ozone_hadoop_catalog"
# Using explicit hostname 'om' which matches docker-compose service name
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
        .config("spark.hadoop.ozone.om.address", "om:9862") \
        .config("spark.hadoop.ozone.scm.client.address", "scm:9860") \
        .config("spark.hadoop.fs.defaultFS", "ofs://om/") \
        .getOrCreate()
    
    # Set log level to INFO
    spark.sparkContext.setLogLevel("INFO")
    
    # Debug: Print all configurations to see if something is interfering
    print("\n--- Spark Configuration ---")
    for k, v in spark.sparkContext.getConf().getAll():
        if "iceberg" in k or "ozone" in k:
            print(f"{k} = {v}")
    print("---------------------------\n")

    return spark

def main():
    # Parse args
    input_file = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_INPUT
    
    # Force local file protocol if not present, to avoid reading from Ozone (defaultFS)
    if not input_file.startswith("file://") and not input_file.startswith("ofs://") and not input_file.startswith("o3fs://"):
        input_file = f"file://{input_file}"
    
    table_name = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_TABLE
    
    # Update catalog name in table reference
    full_table_name = f"{CATALOG_NAME}.default.{table_name}"
    
    print(f"--- Configuration ---")
    print(f"Input File: {input_file}")
    print(f"Target Iceberg Table: {full_table_name}")
    print(f"Warehouse Location: {WAREHOUSE_PATH}")
    print(f"Catalog Name: {CATALOG_NAME}")
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
        
        # Check if we can list the directory first (Connectivity check)
        try:
            print("Checking connectivity to Ozone...")
            fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
            path = spark._jvm.org.apache.hadoop.fs.Path("/vol1/bucket1")
            exists = fs.exists(path)
            print(f"Target bucket exists: {exists}")
        except Exception as conn_err:
            print(f"WARNING: Ozone connectivity check failed: {conn_err}")

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
        print(f"\n[ERROR] Ingestion failed with exception:")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
