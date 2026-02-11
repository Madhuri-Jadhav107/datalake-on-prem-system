from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, DecimalType, DateType
from datetime import date
from decimal import Decimal

def ingest_employee_data():
    print("Initializing Spark Session...")
    spark = SparkSession.builder \
        .appName("IcebergEmployeeIngestion") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hive") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "s3a://warehouse/iceberg_warehouse") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    print("Spark Session created.")

    # Define Schema for Employee
    schema = StructType([
        StructField("id", LongType(), False),
        StructField("name", StringType(), False),
        StructField("department", StringType(), True),
        StructField("salary", DecimalType(10, 2), True),
        StructField("join_date", DateType(), True)
    ])

    # Create Sample Data
    data = [
        (1, "Alice Smith", "Engineering", Decimal("95000.00"), date(2022, 3, 15)),
        (2, "Bob Johnson", "Marketing", Decimal("78000.50"), date(2021, 6, 1)),
        (3, "Charlie Brown", "Sales", Decimal("82000.00"), date(2023, 1, 10)),
    ]

    print("Creating DataFrame...")
    df = spark.createDataFrame(data, schema)
    
    print("Data Preview:")
    df.show()

    # Create/Append to Iceberg Table
    table_name = "local.default.employees"
    
    print(f"Writing to {table_name}...")
    
    try:
        # Create database if not exists
        spark.sql(f"CREATE DATABASE IF NOT EXISTS local.default")
        
        # Check if table exists
        if spark.catalog.tableExists(table_name):
             print("Table exists, appending...")
             df.writeTo(table_name).append()
        else:
             print("Table does not exist, creating...")
             df.writeTo(table_name).create()
             
        print("[SUCCESS] Employee data ingested successfully.")
        
    except Exception as e:
        print(f"[FAILED] Ingestion failed: {e}")

    spark.stop()

if __name__ == "__main__":
    ingest_employee_data()
