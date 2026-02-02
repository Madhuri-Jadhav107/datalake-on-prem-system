from pyspark.sql import SparkSession
import sys

# Create Spark Session (Configs passed via spark-submit)
spark = SparkSession.builder \
    .appName("LargeScaleIngestion") \
    .config("spark.hadoop.fs.ofs.impl", "org.apache.hadoop.fs.ozone.OzoneFileSystem") \
    .config("spark.hadoop.fs.AbstractFileSystem.ofs.impl", "org.apache.hadoop.fs.ozone.OzFs") \
    .getOrCreate()

# Set log level
spark.sparkContext.setLogLevel("ERROR")

# Explicitly set Hadoop configurations in the JVM just in case
sc = spark.sparkContext
sc._jsc.hadoopConfiguration().set("fs.ofs.impl", "org.apache.hadoop.fs.ozone.OzoneFileSystem")
sc._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.ofs.impl", "org.apache.hadoop.fs.ozone.OzFs")

# Parse Arguments
# Usage: script.py [ozone_path_or_local_file] [table_name]
if len(sys.argv) > 1:
    input_path = sys.argv[1]
    # Simple logic: if path starts with /, assume it's inside container or ozone
    # If not, we might need to handle it, but for now expect full URI or container path
    ozone_path = input_path
else:
    ozone_path = "ofs://om/vol1/bucket1/large_sample.csv"

if len(sys.argv) > 2:
    table_name = f"hive_prod.iceberg_db.{sys.argv[2]}"
else:
    table_name = "hive_prod.iceberg_db.large_transactions"

print(f"--- Spark Session Started ---")
print(f"Target Input: {ozone_path}")
print(f"Target Table: {table_name}")

try:
    # Ensure database exists
    spark.sql("CREATE NAMESPACE IF NOT EXISTS hive_prod.iceberg_db")
    
    # Read CSV from Ozone
    df = spark.read.option("header", "true") \
        .option("inferSchema", "true") \
        .csv(ozone_path)

    print("\nSuccess! Ingested data from Ozone.")
    df.show(5)

    # Convert to Iceberg
    print(f"Writing to Iceberg table: {table_name}...")
    df.writeTo(table_name).createOrReplace()

    # Verify
    count = spark.sql(f"SELECT COUNT(*) FROM {table_name}").collect()[0][0]
    print(f"\nFinal Verification: {count} records found in Iceberg table.")

except Exception as e:
    print(f"\n--- INGESTION ERROR ---")
    print(f"Error Message: {e}")
    print("\nAttempting Fallback (Local File)...")
    try:
        # Fallback uses the input path directly assuming it's a file URI or path
        # If input_path was ofs://..., we try to map it to a local file if possible
        # For simplicity in this demo, strict fallback assumes it is /home/iceberg/local/<filename>
        import os
        filename = input_path.split("/")[-1]
        fallback_path = f"file:///home/iceberg/local/{filename}"
        
        print(f"Trying to read from local mount: {fallback_path}")
        
        spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db")
        df_local = spark.read.option("header", "true").csv(fallback_path)
        df_local.writeTo(table_name).createOrReplace()
        print(f"Fallback successful! Table {table_name} created from local file.")
    except Exception as fe:
        print(f"Fallback failed: {fe}")
