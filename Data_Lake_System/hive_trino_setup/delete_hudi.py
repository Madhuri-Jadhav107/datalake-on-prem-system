import argparse
import os
import sys
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# Auto-detect Spark version
spark_version = pyspark.__version__
major_minor = ".".join(spark_version.split(".")[:2])

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
    HUDI_VERSION = '0.14.0'
    SPARK_BUNDLE_VERSION = '3.4'

SUBMIT_ARGS = f"--packages org.apache.hadoop:hadoop-aws:3.3.4,org.apache.hudi:hudi-spark{SPARK_BUNDLE_VERSION}-bundle_2.12:{HUDI_VERSION} pyspark-shell"
os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS
os.environ['PYSPARK_PYTHON'] = sys.executable

def create_spark_session():
    return SparkSession.builder \
        .appName("Hudi_Delete") \
        .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
        .config('spark.sql.extensions', 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension') \
        .config('className', 'org.apache.hudi') \
        .getOrCreate()

def configure_s3(spark):
    spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://localhost:9002")
    spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "minioadmin")
    spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "minioadmin")
    spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    spark._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
    spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

def delete_records(table_name, pkey_field, ids_to_delete):
    spark = create_spark_session()
    configure_s3(spark)
    
    path = f"s3a://warehouse/{table_name}"
    
    print(f"Deleting {len(ids_to_delete)} records from {table_name}...")
    
    # Create a DataFrame with the keys to delete
    # Hudi delete operation expects a dataframe with the same schema as the table, OR just the keys?
    # For Hudi, to delete, we usually just need the Record Key (and Partition Path if it's partitioned).
    # "The DataFrame should contain the record keys and partition path (if any) to be deleted."
    
    # Let's create a simple DF with just the PKey.
    # Note: If partitioned, we might need partition path too. 
    # For now, assuming non-partitioned or user handles it? 
    # Let's try to just pass the PKey.
    
    schema = StructType([StructField(pkey_field, StringType(), False)])
    rows = [[str(x)] for x in ids_to_delete]
    df = spark.createDataFrame(rows, schema=schema)
    
    hudi_options = {
        'hoodie.table.name': table_name,
        'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
        'hoodie.datasource.write.operation': 'delete',
        'hoodie.datasource.write.recordkey.field': pkey_field,
        # Disable metadata table to avoid localhost:9000 issues
        "hoodie.metadata.enable": "false",
        "hoodie.datasource.hive_sync.enable": "true",
        "hoodie.datasource.hive_sync.database": "default",
        "hoodie.datasource.hive_sync.table": table_name,
        "hoodie.datasource.hive_sync.metastore.uris": "thrift://localhost:9084",
        "hoodie.datasource.hive_sync.mode": "hms"
    }

    # If it's a non-partitioned table (common for leads/simple tests here)
    hudi_options["hoodie.datasource.hive_sync.partition_extractor_class"] = "org.apache.hudi.hive.NonPartitionedExtractor"
    hudi_options["hoodie.datasource.write.keygenerator.class"] = "org.apache.hudi.keygen.NonpartitionedKeyGenerator"

    df.write.format("hudi"). \
        options(**hudi_options). \
        mode("append"). \
        save(path)
        
    print("Delete operation complete.")
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True)
    parser.add_argument("--pkey", required=True)
    parser.add_argument("--ids", required=True, help="Comma separated list of IDs")
    
    args = parser.parse_args()
    ids = [x.strip() for x in args.ids.split(",")]
    
    delete_records(args.table, args.pkey, ids)
