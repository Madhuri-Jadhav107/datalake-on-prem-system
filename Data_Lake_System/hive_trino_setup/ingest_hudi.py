import os
import sys
import uuid
import pyspark
from pyspark.sql import SparkSession
from faker import Faker
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType

print("Imports loaded ")

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

# os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@11" # User likely has this set or doesn't need it if running in same env as before
SUBMIT_ARGS = f"--packages org.apache.hadoop:hadoop-aws:3.3.4,org.apache.hudi:hudi-spark{SPARK_BUNDLE_VERSION}-bundle_2.12:{HUDI_VERSION} pyspark-shell"
os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS
os.environ['PYSPARK_PYTHON'] = sys.executable

# Spark session
spark = SparkSession.builder \
    .config('spark.executor.memory', '1g') \
    .config('spark.driver.memory', '1g') \
    .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
    .config('spark.sql.extensions', 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension') \
    .config('className', 'org.apache.hudi') \
    .config('spark.sql.hive.convertMetastoreParquet', 'false') \
    .getOrCreate()

# Use localhost ports for Spark running on host
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://localhost:9002")
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "minioadmin")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "minioadmin")
spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
spark._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")


faker = Faker()

def get_customer_data(total_customers=2):
    customers_array = []
    for i in range(0, total_customers):
        customer_data = {
            "customer_id": str(uuid.uuid4()),
            "name": faker.name(),
            "created_at": datetime.now().isoformat().__str__(),
            "address": faker.address(),
            "state": str(faker.state_abbr()),  # Adding state information
            "salary": str(faker.random_int(min=30000, max=100000))
        }
        customers_array.append(customer_data)
    return customers_array

global total_customers
total_customers = 10
customer_data = get_customer_data(total_customers=total_customers)

# Define schema
schema = StructType([
    StructField("customer_id", StringType(), nullable=True),
    StructField("name", StringType(), nullable=True),
    StructField("created_at", StringType(), nullable=True),
    StructField("address", StringType(), nullable=True),
    StructField("state", StringType(), nullable=True),
    StructField("salary", StringType(), nullable=True)
])

# Create DataFrame
spark_df_customers = spark.createDataFrame(data=customer_data, schema=schema)

# Show DataFrame
spark_df_customers.show(1, truncate=True)

# Print DataFrame schema
spark_df_customers.printSchema()
spark_df = spark_df_customers
database="default"
table_name="customers_t1"

# IMPORTANT: Writing to s3a path directly, Hudi needs location
path = f"s3a://warehouse/{table_name}"

hudi_options = {
    'hoodie.table.name': table_name,
    'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
    'hoodie.datasource.write.operation': 'upsert',
    'hoodie.datasource.write.recordkey.field': 'customer_id',
    'hoodie.datasource.write.precombine.field': 'created_at',
    "hoodie.datasource.write.partitionpath.field": "state",

    "hoodie.enable.data.skipping": "true",
    "hoodie.metadata.enable": "true",
    "hoodie.metadata.index.column.stats.enable": "true",

    "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
    "hoodie.datasource.hive_sync.metastore.uris": "thrift://localhost:9084", 
    "hoodie.datasource.hive_sync.mode": "hms",
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.database": "default",
    "hoodie.datasource.hive_sync.table": table_name,
    "hoodie.datasource.hive_sync.username": "hive",
    "hoodie.datasource.hive_sync.password": "hive",
}

print("\n")
print(f"Writing to {path}")
print("\n")

spark_df.write.format("hudi"). \
    options(**hudi_options). \
    mode("append"). \
    save(path)

print("Ingestion complete. Verify with Trino.")
